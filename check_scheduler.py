import logging
import os
import smtplib
import sys
import time
from datetime import datetime, timedelta
from os import environ as env

import daemon
import dictdiffer
import psycopg2
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

from utils import WorkerParser


class Checker:
    def __init__(self, service_obj):
        self.service = service_obj

    def __get_file_id(self, filename):
        files = self.service.files().list(fields='nextPageToken, files(id, name)').execute()
        for file in files['files']:
            if file['name'] == filename:
                return file['id']
            return

    def check_modified(self, file_id):
        result = self.service.files().get(fileId=file_id, fields='modifiedTime').execute()
        last_modified = result['modifiedTime'].split('.')[0]
        last_modified = datetime.strptime(last_modified, '%Y-%m-%dT%H:%M:%S') + timedelta(hours=3)

        return last_modified

    def was_modified(self):
        compare_time = timedelta(seconds=600)
        time_now = datetime.now().replace(microsecond=0)
        time_modified = self.check_modified(self.__get_file_id(env.get('FILENAME')))
        # print((time_now - time_modified))

        return compare_time > (time_now - time_modified)


def send_email(message):
    '''
    function send email using gmail service
    '''
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.ehlo()

    server.login('email@login', 'email@password')

    sbjct = f'Problem with scheduler parser'
    toAddr = ('--email--address--')
    fromAddr = 'electricsender@gmail.com'
    body = message
    header = 'To:' + ','.join(toAddr) + '\n' + 'From: ' + fromAddr + '\n' + f'Subject:{sbjct}' '\n'
    msg = header + body
    #print(f'MSG {msg}')
    server.sendmail(
        'email@address',
        toAddr,
        msg)

    server.quit()


def check_db():
    connection = psycopg2.connect(dbname=env.get('DB_NAME_2'),
                                  user=env.get('DB_USER'),
                                  password=env.get('DB_PASSWORD'),
                                  host=env.get('HOST'),
                                  port=env.get('PORT'))
    workers = (
        'worker_name',
        'worker_name',
        'worker_name',
        'worker_name',
        'worker_name'
        )

    ou = WorkerParser()
    for i in range(len(workers)):
        # print(workers[i])
        worker_id = ou.get_worker_id(workers[i])
        result = ou.daterange(workers[i], checker=True)
        compare_dict = get_data_from_db(worker_id, result, connection.cursor())

        comparisons = list(dictdiffer.diff(result, compare_dict))
        # print('Comparisons: ', comparisons)
        if comparisons:
            change_db_item(comparisons, connection, worker_id)

        time.sleep(150)


def get_data_from_db(worker, compare_dict, connection_cursor):
    result_dict = {}
    for date in compare_dict:
        query = f"SELECT time_range, task, connection, strike FROM parsing_tasks" \
                f" WHERE worker_id = {worker} AND date = '{date}'"

        connection_cursor.execute(query)
        output = connection_cursor.fetchall()
        result_dict[date] = {item[0]: (item[1], item[2], item[3]) for item in output}

    return result_dict


def change_db_item(compare_list, connection, worker):
    for item in compare_list:
        date, time_, = item[1]
        to_task, to_con, to_strike = item[2][0]
        query = f"UPDATE parsing_tasks SET task = '{to_task}', connection = '{to_con}', strike = '{to_strike}'" \
                f" WHERE worker_id = {worker} AND date = '{date}' and time_range = '{time_}'"

        with connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
        connection.commit()


def main():
    logger = logging.getLogger("CheckerLoger")
    logger.setLevel(logging.INFO)
    log = logging.FileHandler(os.path.join(os.getcwd(), 'CheckerApp.log'))
    formatter = logging.Formatter('{asctime} - {name} - {levelname} - {message}',
                                  datefmt='%Y-%m-%d %H:%M:%S', style='{')
    log.setFormatter(formatter)
    logger.addHandler(log)

    SCOPES = ['https://www.googleapis.com/auth/drive']
    SERVICE_ACCOUNT_FILE = env.get('SERVICE_ACCOUNT_FILE')
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('drive', 'v3', credentials=credentials)
    logger.info(SCOPES[0])
    logger.info(env.get('SERVICE_ACCOUNT_FILE'))

    checker = Checker(service)
    while True:
        try:
            modified = checker.was_modified()
            if modified:
                logger.info(f'modified at {datetime.now()}')
                check_db()
            else:
                time.sleep(600)
        except:
            exc = sys.exc_info()[0]
            logger.info(exc)
            message = f'Exception occured {exc}'
            send_email(message)
            time.sleep(600)
            continue

if __name__ == "__main__":
    load_dotenv()
    with daemon.DaemonContext(working_directory=os.getcwd()):
        main()
