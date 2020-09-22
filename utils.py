import re
import time
import itertools
from datetime import datetime, timedelta
from os import environ as env

import gspread
import gspread_formatting
import psycopg2
from dateutil.rrule import rrule, DAILY
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials


class WorkerParser:
    def __init__(self):
        self.sheet = self.__get_scheduler()
        self.connection = psycopg2.connect(dbname=env.get('DB_NAME_2'), user=env.get('DB_USER'),
                                           password=env.get('DB_PASSWORD'),
                                           host=env.get('HOST'),
                                           port=env.get('PORT'))
        self.cursor = self.connection.cursor()

    def __get_scheduler(self):
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(env.get('SERVICE_ACCOUNT_FILE'), scope)
        client = gspread.authorize(creds)

        sheet = client.open(env.get('FILENAME')).sheet1

        return sheet

    def __worker_cells(self, worker):
        cell = self.sheet.find(f'{worker}')

        return cell.row

    def __task_coordinates(self, task):
        cell = self.sheet.find(f'{task}')

        return cell.col, cell.row

    def __get_column(self, raw_address):
        if raw_address[1].isdigit():
            return raw_address[0]
        return raw_address[:2]

    def get_tasks(self, worker, date_value=None):
        result = {}

        worker_row = self.__worker_cells(worker)

        if date_value:
            date = date_value
        else:
            date = datetime.today().date().strftime("%d.%m.%Y")

        try:
            date_column = self.sheet.find(date).address

        except gspread.exceptions.CellNotFound:
            print(f'На дату {date_value} еще не составлен график')
            return

        column = self.__get_column(date_column)
        column_range = f'{column + str(worker_row)}:{column + str(worker_row + 9)}'
        data = self.sheet.batch_get([column_range])[0]

        time_ranges = [item[0] for item in data[::2]]
        tasks = [item[0] if item else 'Свободная ячейка' for item in data[1::2]]
        complete_data = list(itertools.zip_longest(time_ranges, tasks, fillvalue='Свободная ячейка'))

        connections = self.__is_connection(complete_data)
        strikes = self.__is_strikethrough(complete_data)

        data = [(time_task[0],time_task[1], cn, st) for time_task, cn, st in zip(complete_data, connections, strikes)]

        for t_time, t_value, t_con, t_strike in data:
            result[self.__pretty_time(t_time)] = (t_value, t_con, t_strike)

        return result

    def get_date_tasks(self, worker, date_value):
        if not self.__check_date_format(date_value):
            print('Неверный формат даты. Формат даты 01.10.2000(день.месяц.год)')
            return

        return self.get_tasks(worker, date_value)

    def get_last_date(self, worker):
        query = f"SELECT date FROM parsing_tasks join parsing_workers on parsing_tasks.worker_id=parsing_workers.id" \
                f" where parsing_workers.full_name = '{worker}' ORDER BY date DESC LIMIT 1"
        self.cursor.execute(query)
        return self.cursor.fetchall()[0][0]

    def __working_day(self,raw_column_data):
        data = raw_column_data[1]
        if not data or data[0].isdigit():
            return [True] * 5
        return [False] * 5

    def __is_connection(self, raw_task_list):
        task_list = [item[1] for item in raw_task_list]
        task_data = []

        for task in task_list:
            try:
                task = int(task)
            except ValueError:
                task = 0
            task_data.append(task)
        result = [i < 80000 and i != 0 for i in task_data]

        return result

    def __is_strikethrough(self, raw_list):
        options = ('вых', 'Свободная ячейка', 'не занимать', 'болен')
        task_list = [item[1] for item in raw_list]
        result = []

        for task in task_list:
            if task in options:
                result.append(False)
                continue
            task_address = self.sheet.find(task).address
            res = gspread_formatting.get_effective_format(self.sheet, task_address)
            result.append(res.textFormat.strikethrough)
        return result

    def __check_date_format(self, date_value):
        date_format = "%d.%m.%Y"
        try:
            datetime.strptime(date_value, date_format)
            return True

        except ValueError:
            return False

    def __pretty_time(self, time_value):
        if ' - ' in time_value:
            pass
        elif ' -' in time_value:
            time_value = time_value.replace(' -', ' - ')
        elif '- ' in time_value:
            time_value = time_value .replace('- ', ' - ')

        return time_value

    def __get_last_date(self):
        regexp = re.compile(r'\d+.\d+.2020')
        result = self.sheet.findall(regexp)

        return result[-1].value

    def get_worker_id(self, worker):
        query = f"SELECT id from parsing_workers where full_name = '{worker}'"
        self.cursor.execute(query)
        worker_id = self.cursor.fetchone()[0]

        return worker_id

    def __column_string(self,n):
        string = ""

        while n > 0:
            n, remainder = divmod(n - 1, 26)
            string = chr(65 + remainder) + string
        return string

    def daterange(self, worker, checker=None):
        start_date = datetime.today()
        if checker:
            end_date = datetime.today() + timedelta(days=2)
        else:
            end_date = datetime.today().date() + timedelta(days=3)
        result = {}

        for dt in rrule(DAILY, dtstart=start_date, until=end_date):
            output = self.get_date_tasks(worker, dt.strftime("%d.%m.%Y"))
            if output:
                result[dt.date()] = output
        return result


def commit_db():
    connection = psycopg2.connect(dbname=env.get('DB_NAME_2'),
                                  user=env.get('DB_USER'),
                                  password=env.get('DB_PASSWORD'),
                                  host=env.get('HOST'),
                                  port=env.get('PORT'))

    cursor = connection.cursor()
    cursor.execute(f'select full_name from {env.get("WORKER_TABLE")}')
    workers = [name[0] for name in cursor.fetchall()]

    ou = WorkerParser()
    for i in range(len(workers)):
        result = ou.daterange(workers[i])
        for date in result:
            for time_range in result[date]:
                with connection:
                    cursor.execute(f"SELECT id FROM parsing_workers WHERE full_name = '{workers[i]}'")
                    worker_id = cursor.fetchone()[0]
                    cursor.execute(
                        f"INSERT INTO parsing_tasks(worker_id,time_range,date,task,connection,strike) VALUES({worker_id},"
                        f"'{time_range}',"
                        f"'{date}',"
                        f"'{result[date][time_range][0]}', '{result[date][time_range][1]}','{result[date][time_range][2]}')")
                connection.commit()

        time.sleep(150)


if __name__ == "__main__":
    load_dotenv()
    commit_db()