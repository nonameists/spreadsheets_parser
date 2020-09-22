import csv
from datetime import datetime
from datetime import timedelta
from os import environ as env

import dictdiffer
import gspread
import gspread_formatting
import psycopg2
from dateutil.rrule import rrule, DAILY
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
load_dotenv()


class CsvParser:
    def __init__(self):
        self.sheet, self.csv_file = self.__create_csv()
        self.connection = psycopg2.connect(dbname=env.get('DB_NAME_2'),
                                      user=env.get('DB_USER'),
                                      password=env.get('DB_PASSWORD'),
                                      host=env.get('HOST'),
                                      port=env.get('PORT'))

        self.cursor = self.connection.cursor()
        self.workers, self.workers_id = self.__workers()


    def get_day_index(self, date: str):
        '''
        return date index
        '''
        return self.csv_file[0].index(date)

    def get_worker_index(self, worker: str):
        '''
        return tuple with (worker_start_index, worker_end_index)
        '''
        idx = 0
        for row in self.csv_file:
            if row[0] == worker:
                return idx, idx + 9
            idx += 1
        return None

    def get_day_tasks(self, worker: str, date: str):

        date_index = self.get_day_index(date)
        result = {}
        raw_data = []
        w_start_index, w_end_index = self.get_worker_index(worker)

        for i in range(w_start_index, w_end_index + 1):
            raw_data.append(self.csv_file[i][date_index])
        time_ = [self.__pretty_time(time_range) for time_range in raw_data[::2]]
        if not time_[-1]:
            time_.pop()
        tasks = [task if task else 'Свободная ячейка' for task in raw_data[1::2]]
        connections = self.__is_connection(tasks)
        strikes = self.__is_strikethrough(tasks)
        if len(time_) != len(connections):
            connections = connections[:len(time_)]

        for tm, ts, con, st in zip(time_, tasks, connections, strikes):
            if tm:
                result[tm] = (ts, con, st)
        return result

    def __pretty_time(self, time_value):
        if ' - ' in time_value:
            pass
        elif ' -' in time_value:
            time_value = time_value.replace(' -', ' - ')
        elif '- ' in time_value:
            time_value = time_value.replace('- ', ' - ')
        return time_value

    def __is_connection(self, raw_task_list):
        task_list = [item for item in raw_task_list]
        task_data = []

        for task in task_list:
            try:
                task = int(task)
            except ValueError:
                task = 0
            task_data.append(task)
        result = [i < 80000 and i != 0 for i in task_data]

        return result

    def __is_strikethrough(self, task_list):
        options = ('вых', 'свободная ячейка', 'не занимать', 'болен')
        result = []

        for task in task_list:
            if task.lower() in options:
                result.append(False)
                continue
            task_address = self.sheet.find(task).address
            res = gspread_formatting.get_effective_format(self.sheet, task_address)
            result.append(res.textFormat.strikethrough)
        return result

    def __create_csv(self):
        result = []
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(env.get('SERVICE_ACCOUNT_FILE'), scope)
        client = gspread.authorize(creds)

        sheet = client.open(env.get('FILENAME')).sheet1

        df = pd.DataFrame(sheet.get_all_records())
        filename = sheet.title + f"_{datetime.now()}" + '.csv'
        df.to_csv(filename, index=False)

        with open(filename) as f:
            reader = csv.reader(f)
            for row in reader:
                result.append(row)

        return sheet, result

    def daterange(self, worker, checker=None):
        start_date = datetime.today()
        if checker:
            end_date = datetime.today() + timedelta(days=2)
        else:
            end_date = datetime.strptime(self.csv_file[0][-1], '%d.%m.%Y').date() + timedelta(days=1)
        result = {}

        for dt in rrule(DAILY, dtstart=start_date, until=end_date):
            output = self.get_day_tasks(worker, dt.strftime("%d.%m.%Y"))
            if output:
                result[dt.date()] = output
        return result

    def __workers(self):
        self.cursor.execute(f'SELECT full_name FROM {env.get("WORKER_TABLE")}')
        workers = [name[0] for name in self.cursor.fetchall()]
        workers_id = {name: idx for idx, name in enumerate(workers, 1)}

        return workers, workers_id

    def get_data_from_db(self, worker, compare_dict):
        worker_id = self.workers_id[worker]
        result_dict = {}
        for date in compare_dict:
            query = f"SELECT time_range, task, connection, strike FROM parsing_tasks" \
                    f" WHERE worker_id = {worker_id} AND date = '{date}'"

            self.cursor.execute(query)
            output = self.cursor.fetchall()
            result_dict[date] = {item[0]: (item[1], item[2], item[3]) for item in output}

        return result_dict

    def change_db_item(self, compare_list, worker):
        worker_id = self.workers_id[worker]
        for item in compare_list:
            date, time_, = item[1]
            to_task, to_con, to_strike = item[2][0]
            query = f"UPDATE parsing_tasks SET task = '{to_task}', connection = '{to_con}', strike = '{to_strike}'" \
                    f" WHERE worker_id = {worker_id} AND date = '{date}' and time_range = '{time_}'"

            with self.connection:
                self.cursor.execute(query)
            self.connection.commit()


def main():
    parser = CsvParser()

    for worker in parser.workers:
        result = parser.daterange(worker)
        for date in result:
            for time_range in result[date]:
                with parser.connection:
                    parser.cursor.execute(f"SELECT id FROM parsing_workers WHERE full_name = '{worker}'")
                    worker_id = parser.cursor.fetchone()[0]
                    parser.cursor.execute(
                        f"INSERT INTO parsing_tasks(worker_id,time_range,date,task,connection,strike) VALUES({worker_id},"
                        f"'{time_range}',"
                        f"'{date}',"
                        f"'{result[date][time_range][0]}', '{result[date][time_range][1]}','{result[date][time_range][2]}')")
                parser.connection.commit()


if __name__ == "__main__":
    main()





