from Google import Create_Service
import os
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
from os import environ as env
from check_strikes import StrikeChecker


class StatWorker:
    def __init__(self, oauth2_secret):
        self.oauth2_secret = oauth2_secret
        folder_path = os.getcwd()
        client_secret_file = os.path.join(folder_path, self.oauth2_secret)
        api_service_name = 'sheets'
        api_version = 'v4'
        scopes = ['https://spreadsheets.google.com/feeds']
        self.service = Create_Service(client_secret_file, api_service_name, api_version, scopes)
        self.sheet = self.__create_spreadsheet()
        self.title = self.sheet['sheets'][0]['properties']['title']
        self.cell_range_insert = 'A1'

        self.connection = psycopg2.connect(dbname=env.get('DB_NAME_2'), user=env.get('DB_USER'),
                                           password=env.get('DB_PASSWORD'),
                                           host=env.get('HOST'),
                                           port=env.get('PORT'))
        self.cursor = self.connection.cursor()

        drive_service_name = 'drive'
        drive_api_version = 'v3'
        drive_scopes = ['https://www.googleapis.com/auth/drive']
        self.drive_service = Create_Service(client_secret_file, drive_service_name, drive_api_version, drive_scopes)


    def __create_spreadsheet(self):
        spreadsheet_body = {
            'properties': {
                'title': f"{datetime.now().strftime('%B-%Y')} statistics"
            }
        }
        sheet = self.service.spreadsheets().create(body=spreadsheet_body).execute()

        return sheet

    def set_bold_and_center(self, row, last_column, font_size):
        body_bold = {
            "requests": [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": self.sheet['sheets'][0]['properties']['sheetId'],
                            "startRowIndex": row - 1,
                            "endRowIndex": row,
                            "startColumnIndex": 0,
                            "endColumnIndex": last_column

                        },
                        "cell": {
                            "userEnteredFormat": {
                                "horizontalAlignment": "CENTER",
                                "textFormat": {
                                    "fontSize": font_size,
                                    "bold": True
                                }
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
                    }
                },
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": self.sheet['sheets'][0]['properties']['sheetId'],
                            "gridProperties": {
                                "frozenRowCount": 1
                            }
                        },
                        "fields": "gridProperties.frozenRowCount"
                    }
                }
            ]
        }

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.sheet['spreadsheetId'],
            body=body_bold
        ).execute()

    def set_borders(self, row, end_row, last_column):
        body_borders = {
            'requests': [
                {
                    'updateBorders': {
                        'range': {
                            'sheetId': self.sheet['sheets'][0]['properties']['sheetId'],
                            'startRowIndex': row + 1,
                            'endRowIndex': end_row,
                            'startColumnIndex': 0,
                            'endColumnIndex': last_column
                        },
                        'bottom': {
                            'style': 'SOLID',
                            'width': 1,
                            'color': {
                                'red': 0,
                                'green': 0,
                                'blue': 0,
                            }
                        },
                        'top': {
                            'style': 'SOLID',
                            'width': 1,
                            'color': {
                                'red': 0,
                                'green': 0,
                                'blue': 0,
                            }
                        },
                        'left': {
                            'style': 'SOLID',
                            'width': 1,
                            'color': {
                                'red': 0,
                                'green': 0,
                                'blue': 0,
                            }
                        },
                        'right': {
                            'style': 'SOLID',
                            'width': 1,
                            'color': {
                                'red': 0,
                                'green': 0,
                                'blue': 0,
                            }
                        },
                        'innerHorizontal': {
                            'style': 'SOLID',
                            'width': 1,
                            'color': {
                                'red': 0,
                                'green': 0,
                                'blue': 0,
                            }
                        },
                        'innerVertical': {
                            'style': 'SOLID',
                            'width': 1,
                            'color': {
                                'red': 0,
                                'green': 0,
                                'blue': 0,
                            }
                        },
                    }
                },
            ]
        }

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.sheet['spreadsheetId'],
            body=body_borders
        ).execute()

    def insert_data(self, values, row):
        values_body = {
            'majorDimension': 'ROWS',
            'values': values,
        }

        self.service.spreadsheets().values().update(
            spreadsheetId=self.sheet['spreadsheetId'],
            valueInputOption='USER_ENTERED',
            range=self.title + '!' + 'A' + str(row),
            body=values_body
        ).execute()

    def append_data(self, values):
        values_body = {
            'majorDimension': 'ROWS',
            'values': values,
        }

        self.service.spreadsheets().values().append(
            spreadsheetId=self.sheet['spreadsheetId'],
            valueInputOption='USER_ENTERED',
            range=self.title + '!' + self.cell_range_insert,
            body=values_body
        ).execute()

    def get_last_row_number(self):
        result = self.service.spreadsheets().values().get(spreadsheetId=self.sheet['spreadsheetId'],
                                                          range="Sheet1!A1:A").execute()
        return len(result.get('values'))

    def get_non_con(self):
        query = "SELECT parsing_workers.full_name, date, task FROM parsing_tasks JOIN parsing_workers ON" \
                          " parsing_tasks.worker_id=parsing_workers.id WHERE task ~ '^[1-9]' AND connection = 't'" \
                          " AND strike = 'f' AND date < (SELECT CURRENT_DATE) ORDER BY full_name, date"
        self.cursor.execute(query)

        result = self.cursor.fetchall()
        result = [(item[0], str(item[1]), item[2]) for item in result]

        if result:
            start_row = self.get_last_row_number()
            headers = [('Монтажник', 'Дата', 'Незачеркнутые вовремя подключения')]
            self.insert_data(headers, start_row + 3)

            self.set_bold_and_center(self.get_last_row_number(), 3, 10)
            self.insert_data(result, self.get_last_row_number() + 1)

            self.set_borders(start_row + 1, self.get_last_row_number(), 3)

            return result

    def get_non_fix(self):
        query = "SELECT parsing_workers.full_name, date, task FROM parsing_tasks JOIN parsing_workers ON" \
                          " parsing_tasks.worker_id=parsing_workers.id WHERE task ~ '^[1-9]' AND connection = 'f'" \
                          " AND strike = 'f' AND date < (SELECT CURRENT_DATE) ORDER BY full_name, date"
        self.cursor.execute(query)

        result = self.cursor.fetchall()
        result = [(item[0], str(item[1]), item[2]) for item in result]

        if result:
            start_row = self.get_last_row_number()
            headers = [('Монтажник', 'Дата', 'Незачеркнутые вовремя ремонты')]
            self.insert_data(headers, start_row + 3)

            self.set_bold_and_center(self.get_last_row_number(), 3, 10)
            self.insert_data(result, self.get_last_row_number() + 1)

            self.set_borders(start_row + 1, self.get_last_row_number(), 3)

            return result

    def check_current_sheet(self, tasks, work_type):
        checker = StrikeChecker()
        result = [item for item in tasks if not checker.is_strike_current(item[2])]

        if result:
            headers = [('Монтажник', 'Дата', f'Незачеркнутые {work_type} факт')]
            start_row = self.get_last_row_number()
            self.insert_data(headers, start_row + 3)

            self.set_bold_and_center(self.get_last_row_number(), 3, 10)
            self.insert_data(result, self.get_last_row_number() + 1)
            self.set_borders(start_row + 1, self.get_last_row_number(), 3)

    def share_spreadsheet(self):
        user_permission = {
            'type': 'user',
            'role': 'writer',
            'emailAddress': 'main@mk-net.ru'
            }
        self.drive_service.permissions().create(
            fileId=self.sheet['spreadsheetId'],
            body=user_permission).execute()

    def fill_data(self):
        query = "SELECT t1.full_name, t1.total_connections, COALESCE(t2.non_strike, 0) AS non_strike_connection," \
                " COALESCE(t3.fixes, 0) AS fixes, COALESCE(t4.non_strike_fixes, 0) AS Non_strike_fixes" \
                " FROM (select parsing_workers.full_name, count(task) as total_connections from parsing_tasks" \
                " join parsing_workers on parsing_workers.id = parsing_tasks.worker_id where task ~ '^[0-9]'" \
                " and connection='t' AND date < (SELECT CURRENT_DATE) GROUP BY parsing_workers.full_name) t1" \
                " LEFT JOIN (select parsing_workers.full_name, count(task) as non_strike from parsing_tasks" \
                " join parsing_workers on parsing_workers.id = parsing_tasks.worker_id where task ~ '^[0-9]'" \
                " and connection='t' and strike='f' AND date < (SELECT CURRENT_DATE) GROUP BY parsing_workers.full_name) t2 ON (t1.full_name = t2.full_name)" \
                " LEFT JOIN (select parsing_workers.full_name, count(task) as fixes from parsing_tasks join parsing_workers " \
                "on parsing_workers.id = parsing_tasks.worker_id where task ~ '^[0-9]' and connection='f' and strike='t'" \
                " GROUP BY parsing_workers.full_name) t3 ON (t1.full_name = t3.full_name)" \
                " LEFT JOIN (select parsing_workers.full_name, count(task) as non_strike_fixes from parsing_tasks" \
                " join parsing_workers on parsing_workers.id = parsing_tasks.worker_id where task ~ '^[0-9]'" \
                " and connection='f' and strike='f' AND date < (SELECT CURRENT_DATE) GROUP BY parsing_workers.full_name) t4 ON (t1.full_name = t4.full_name)" \
                " ORDER BY t1.total_connections DESC;"
        self.cursor.execute(query)
        values = [('Монтажник', 'Всего подключений', 'Незачеркнутые подключения', 'Технички', 'Незачеркнутые технички')]
        values.extend(self.cursor.fetchall())

        value_range_body = {

            'majorDimension': 'ROWS',

            'values': values

        }

        self.service.spreadsheets().values().update(
            spreadsheetId=self.sheet['spreadsheetId'],
            valueInputOption='USER_ENTERED',
            range=self.title + '!' + self.cell_range_insert,
            body=value_range_body
        ).execute()

        self.set_borders(0, 6, 5)
        self.set_bold_and_center(1, 5, 12)

        # set autosize for columns
        body = {
            'requests': [
                {
                    'autoResizeDimensions': {
                        'dimensions': {
                            'sheetId': self.sheet['sheets'][0]['properties']['sheetId'],
                            'dimension': 'COLUMNS',
                            'startIndex': 0,
                            'endIndex': 6
                        }
                    }
                }
            ]
        }
        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.sheet['spreadsheetId'],
            body=body
        ).execute()

        # generate charts
        charts_body = {
            "requests": [
                {
                    "addChart": {
                        "chart": {
                            "spec": {
                                "title": f"График за {datetime.now().month}-{datetime.now().year}",
                                "basicChart": {
                                    "chartType": "COLUMN",
                                    "legendPosition": "BOTTOM_LEGEND",
                                    "axis": [
                                        {
                                            "position": "BOTTOM_AXIS",
                                            "title": "Монтажник"
                                        },
                                        {
                                            "position": "LEFT_AXIS",
                                            "title": "Колличество"
                                        }
                                    ],
                                    "domains": [
                                        {
                                            "domain": {
                                                "sourceRange": {
                                                    "sources": [
                                                        {
                                                            "sheetId": self.sheet['sheets'][0]['properties']['sheetId'],
                                                            "startRowIndex": 0,
                                                            "endRowIndex": 7,
                                                            "startColumnIndex": 0,
                                                            "endColumnIndex": 1
                                                        }
                                                    ]
                                                }
                                            }
                                        }
                                    ],
                                    "series": [
                                        {
                                            "series": {
                                                "sourceRange": {
                                                    "sources": [
                                                        {
                                                            "sheetId": self.sheet['sheets'][0]['properties']['sheetId'],
                                                            "startRowIndex": 0,
                                                            "endRowIndex": 7,
                                                            "startColumnIndex": 1,
                                                            "endColumnIndex": 2
                                                        }
                                                    ]
                                                }
                                            },
                                            "targetAxis": "LEFT_AXIS"
                                        },
                                        {
                                            "series": {
                                                "sourceRange": {
                                                    "sources": [
                                                        {
                                                            "sheetId": self.sheet['sheets'][0]['properties']['sheetId'],
                                                            "startRowIndex": 0,
                                                            "endRowIndex": 7,
                                                            "startColumnIndex": 2,
                                                            "endColumnIndex": 3
                                                        }
                                                    ]
                                                }
                                            },
                                            "targetAxis": "LEFT_AXIS"
                                        },
                                        {
                                            "series": {
                                                "sourceRange": {
                                                    "sources": [
                                                        {
                                                            "sheetId": self.sheet['sheets'][0]['properties']['sheetId'],
                                                            "startRowIndex": 0,
                                                            "endRowIndex": 7,
                                                            "startColumnIndex": 3,
                                                            "endColumnIndex": 4
                                                        }
                                                    ]
                                                }
                                            },
                                            "targetAxis": "LEFT_AXIS"
                                        },
                                        {
                                            "series": {
                                                "sourceRange": {
                                                    "sources": [
                                                        {
                                                            "sheetId": self.sheet['sheets'][0]['properties']['sheetId'],
                                                            "startRowIndex": 0,
                                                            "endRowIndex": 7,
                                                            "startColumnIndex": 4,
                                                            "endColumnIndex": 5
                                                        }
                                                    ]
                                                }
                                            },
                                            "targetAxis": "LEFT_AXIS"
                                        }
                                    ],
                                    "headerCount": 1
                                }
                            },
                            "position": {
                                "newSheet": True
                            }
                        }
                    }
                }
            ]
        }

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.sheet['spreadsheetId'],
            body=charts_body
        ).execute()

        # generate non strike tables
        nf_con = self.get_non_con()
        nf_fix = self.get_non_fix()

        # check non strike tasks in current sheet
        self.check_current_sheet(nf_con, 'подключения')
        self.check_current_sheet(nf_fix, 'ремонты')

        self.share_spreadsheet()


if __name__ == "__main__":
    load_dotenv()

    parser = StatWorker(env.get('SERVICE_OAUTH_FILE'))
    parser.fill_data()

