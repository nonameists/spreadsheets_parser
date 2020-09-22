from Google import Create_Service
import os
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
from os import environ as env

load_dotenv()

FOLDER_PATH = os.getcwd()
CLIENT_SECRET_FILE = os.path.join(FOLDER_PATH, 'client_secret_oa.json')
API_SERVICE_NAME = 'sheets'
API_VERSION = 'v4'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

service = Create_Service(CLIENT_SECRET_FILE, API_SERVICE_NAME, API_VERSION, SCOPES)

month = datetime.now().month
year = datetime.now().year
spreadsheet_body = {
    'properties': {
        'title': f"{datetime.now().strftime('%B-%Y')} statistics"
    }
}

sheet = service.spreadsheets().create(body=spreadsheet_body).execute()

spread_id = sheet['spreadsheetId']
worksheet_name = 'Sheet1!'

cell_range_insert = 'A1'


def set_bold_and_center(service_object, row):
    body_bold = {
      "requests": [
        {
          "repeatCell": {
            "range": {
              "sheetId":0,
              "startRowIndex": row-1,
              "endRowIndex": row,
              "startColumnIndex": 0,
              "endColumnIndex": 3

            },
            "cell": {
              "userEnteredFormat": {
                "horizontalAlignment": "CENTER",
                "textFormat": {
                  "fontSize": 10,
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
              "sheetId": 0,
              "gridProperties": {
                "frozenRowCount": 1
              }
            },
            "fields": "gridProperties.frozenRowCount"
          }
        }
      ]
    }

    service_object.spreadsheets().batchUpdate(
        spreadsheetId=spread_id,
        body=body_bold
    ).execute()


def set_borders(service_object, row, end_row):
    body_borders = {
        'requests': [
            {
                'updateBorders': {
                    'range': {
                        'sheetId': 0,
                        'startRowIndex': row+1,
                        'endRowIndex': end_row,
                        'startColumnIndex': 0,
                        'endColumnIndex': 3
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

    service_object.spreadsheets().batchUpdate(
        spreadsheetId=spread_id,
        body=body_borders
    ).execute()


def insert_data(service_object, values, spreadsheet_id, row):
    values_body = {
        'majorDimension': 'ROWS',
        'values': values,
    }

    service_object.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        valueInputOption='USER_ENTERED',
        range=worksheet_name+'A'+str(row),
        body=values_body
        ).execute()


def append_data(service_object, values, spreadsheet_id):
    values_body = {
        'majorDimension': 'ROWS',
        'values': values,
    }

    service_object.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        valueInputOption='USER_ENTERED',
        range=worksheet_name+cell_range_insert,
        body=values_body
        ).execute()


def get_last_row_number(service_object, spreadsheet_id):
    result = service_object.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range="Sheet1!A1:A").execute()
    return len(result.get('values'))


def get_non_strike(service_object, cursor_object, ss_id):
    non_stike_con_q = "SELECT parsing_workers.full_name, date, task FROM parsing_tasks JOIN parsing_workers ON" \
                      " parsing_tasks.worker_id=parsing_workers.id WHERE task ~ '^[1-9]' AND connection = 't'" \
                      " AND strike = 'f' ORDER BY full_name, date"
    cursor_object.execute(non_stike_con_q)

    result = cursor.fetchall()
    result = [(item[0], str(item[1]), item[2]) for item in result]

    if result:
        start_row = get_last_row_number(service_object, ss_id)
        headers = [('Монтажник', 'Дата', 'Незачеркнутые вовремя подключения')]
        insert_data(service_object, headers, ss_id, start_row+3)

        last_row = get_last_row_number(service_object, ss_id)
        set_bold_and_center(service_object, last_row)
        insert_data(service_object, result, ss_id, last_row+1)

        last_row = get_last_row_number(service_object, ss_id)
        set_borders(service_object, start_row+1,last_row)


def get_non_fix(service_object, cursor_object, ss_id):
    non_stike_con_q = "SELECT parsing_workers.full_name, date, task FROM parsing_tasks JOIN parsing_workers ON" \
                      " parsing_tasks.worker_id=parsing_workers.id WHERE task ~ '^[1-9]' AND connection = 'f'" \
                      " AND strike = 'f' ORDER BY full_name, date"
    cursor_object.execute(non_stike_con_q)

    result = cursor.fetchall()
    result = [(item[0], str(item[1]), item[2]) for item in result]

    if result:
        start_row = get_last_row_number(service_object, ss_id)
        headers = [('Монтажник', 'Дата', 'Незачеркнутые вовремя ремонты')]
        insert_data(service_object, headers, ss_id, start_row+3)

        last_row = get_last_row_number(service_object, ss_id)
        set_bold_and_center(service_object, last_row)
        insert_data(service_object, result, ss_id, last_row+1)

        last_row = get_last_row_number(service_object, ss_id)
        set_borders(service_object, start_row + 1, last_row)


connection = psycopg2.connect(dbname=env.get('DB_NAME_2'), user=env.get('DB_USER'),
                              password=env.get('DB_PASSWORD'),
                              host=env.get('HOST'),
                              port=env.get('PORT'))

cursor = connection.cursor()

query = "SELECT t1.full_name, t1.total_connections, COALESCE(t2.non_strike, 0) AS non_strike_connection," \
        " COALESCE(t3.fixes, 0) AS fixes, COALESCE(t4.non_strike_fixes, 0) AS Non_strike_fixes" \
        " FROM (select parsing_workers.full_name, count(task) as total_connections from parsing_tasks" \
        " join parsing_workers on parsing_workers.id = parsing_tasks.worker_id where task ~ '^[0-9]'" \
        " and connection='t' AND date <= (SELECT CURRENT_DATE) GROUP BY parsing_workers.full_name) t1" \
        " LEFT JOIN (select parsing_workers.full_name, count(task) as non_strike from parsing_tasks" \
        " join parsing_workers on parsing_workers.id = parsing_tasks.worker_id where task ~ '^[0-9]'" \
        " and connection='t' and strike='f' AND date <= (SELECT CURRENT_DATE) GROUP BY parsing_workers.full_name) t2 ON (t1.full_name = t2.full_name)" \
        " LEFT JOIN (select parsing_workers.full_name, count(task) as fixes from parsing_tasks join parsing_workers " \
        "on parsing_workers.id = parsing_tasks.worker_id where task ~ '^[0-9]' and connection='f' and strike='t'" \
        " GROUP BY parsing_workers.full_name) t3 ON (t1.full_name = t3.full_name)" \
        " LEFT JOIN (select parsing_workers.full_name, count(task) as non_strike_fixes from parsing_tasks" \
        " join parsing_workers on parsing_workers.id = parsing_tasks.worker_id where task ~ '^[0-9]'" \
        " and connection='f' and strike='f' AND date <= (SELECT CURRENT_DATE) GROUP BY parsing_workers.full_name) t4 ON (t1.full_name = t4.full_name)" \
        " ORDER BY t1.total_connections DESC;"

cursor.execute(query)


# заполнение данными
values = [('Монтажник', 'Всего подключений', 'Незачеркнутые подключения', 'Технички', 'Незачеркнутые технички')]
values.extend(cursor.fetchall())

value_range_body = {

    'majorDimension': 'ROWS',

    'values': values

}

service.spreadsheets().values().update(
    spreadsheetId=spread_id,
    valueInputOption='USER_ENTERED',
    range=worksheet_name + cell_range_insert,
    body=value_range_body
    ).execute()



# создать borders
body_borders = {
    'requests': [
        {
            'updateBorders': {
                'range': {
                    'sheetId': 0,
                    'startRowIndex': 0,
                    'endRowIndex': 6,
                    'startColumnIndex': 0,
                    'endColumnIndex': 5
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

service.spreadsheets().batchUpdate(
    spreadsheetId=spread_id,
    body=body_borders
).execute()


# set first row bold and centered
body_bold = {
  "requests": [
    {
      "repeatCell": {
        "range": {
          "sheetId":0,
          "startRowIndex": 0,
          "endRowIndex": 1,
          "startColumnIndex": 0,
          "endColumnIndex": 5

        },
        "cell": {
          "userEnteredFormat": {
            "horizontalAlignment": "CENTER",
            "textFormat": {
              "fontSize": 12,
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
          "sheetId": 0,
          "gridProperties": {
            "frozenRowCount": 1
          }
        },
        "fields": "gridProperties.frozenRowCount"
      }
    }
  ]
}

service.spreadsheets().batchUpdate(
    spreadsheetId=spread_id,
    body=body_bold
).execute()

# авто размер столбоц
body = {
    'requests': [
        {
            'autoResizeDimensions': {
                'dimensions': {
                    'sheetId': 0,
                    'dimension': 'COLUMNS',
                    'startIndex': 0,
                    'endIndex': 6
                }
            }
        }
    ]
}

service.spreadsheets().batchUpdate(
    spreadsheetId=spread_id,
    body=body
).execute()

# create charts
charts_body = {
  "requests": [
    {
      "addChart": {
        "chart": {
          "spec": {
            "title": f"График за {month}-{year}",
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
                          "sheetId": 0,
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
                          "sheetId": 0,
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
                          "sheetId": 0,
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
                          "sheetId": 0,
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
                          "sheetId": 0,
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

service.spreadsheets().batchUpdate(
    spreadsheetId=spread_id,
    body=charts_body
).execute()


get_non_strike(service, cursor, spread_id)
get_non_fix(service, cursor, spread_id)