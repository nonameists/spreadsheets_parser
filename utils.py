#!/usr/bin/env python
import re
import time
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

    def get_tasks(self, worker, date_value=None):
        result = {}

        worker_row = self.__worker_cells(worker)

        if date_value:
            date = date_value
        else:
            date = datetime.today().date().strftime("%d.%m.%Y")

        try:
            date_column = self.sheet.find(date).col

        except gspread.exceptions.CellNotFound:
            print(f'На дату {date_value} еще не составлен график')
            return

        raw_data = self.sheet.range(worker_row, date_column, worker_row + 9, date_column)
        time_data = raw_data[::2]
        task_data = raw_data[1::2]
        connections = self.__is_connection(task_data)
        strikes = self.__is_strikethrough(task_data)

        data = [(time_.value, task.value, cn, st)
                for time_, task, cn, st in zip(time_data, task_data, connections, strikes)
                if time_.value]

        for t_time, t_value, t_con, t_strike in data:
            if not t_value:
                t_value = 'Свободная ячейка'
            result[self.__pretty_time(t_time)] = (t_value, t_con, t_strike)

        return result

    def get_date_tasks(self, worker, date_value):
        if not self.__check_date_format(date_value):
            print('Неверный формат даты. Формат даты 01.10.2000(день.месяц.год)')
            return

        return self.get_tasks(worker, date_value)

    def next_two_weeks_tasks(self):
        pass

    def __is_connection(self, raw_task_list):
        task_data = []
        for task in raw_task_list:
            try:
                task = int(task.value)
            except ValueError:
                task = 0
            task_data.append(task)
        print('task_data', task_data)
        result = [i < 80000 and i != 0 for i in task_data]
        print('is connection result', result)

        return result

    def __is_strikethrough(self, raw_task_list):
        result = []
        for task in raw_task_list:
            if not task.value:
                result.append(False)
                continue
            task_column, task_row = self.__task_coordinates(task.value)
            converted_column = self.__column_string(task_column)
            res = gspread_formatting.get_effective_format(self.sheet, converted_column+str(task_row))
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
            self.cursor.execute('SELECT date FROM parsing_tasks ORDER BY date DESC LIMIT 1')
            end_date = str(self.cursor.fetchone()[0])
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
            print('END DATE', end_date)
        else:
            end_date = datetime.today() + timedelta(days=7)
        result = {}

        for dt in rrule(DAILY, dtstart=start_date, until=end_date):
            print(dt.strftime("%d.%m.%Y"))
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
    for i in range(5):
        result = ou.daterange(workers[i])
        for date in result:
            for time_range in result[date]:
                with connection:
                    with connection.cursor() as cursor:
                        cursor.execute(f"SELECT id FROM parsing_workers WHERE full_name = '{workers[i]}'")
                        worker_id = cursor.fetchone()[0]
                        cursor.execute(
                            f"INSERT INTO parsing_tasks(worker_id,time_range,date,task,connection,strike) VALUES({worker_id},"
                            f"'{time_range}',"
                            f"'{date}',"
                            f"'{result[date][0]}', '{result[date][1]}','{result[date][2]}')"
                        )
                connection.commit()

        time.sleep(150)


if __name__ == "__main__":
    load_dotenv()
    commit_db()
