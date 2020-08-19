from os import environ as env

import gspread
import gspread_formatting
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials


class StrikeChecker:
    def __init__(self):
        self.sheet = self.__get_scheduler()

    def __get_scheduler(self, share=None):
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(env.get('SERVICE_ACCOUNT_FILE'), scope)
        client = gspread.authorize(creds)
        sheet = client.open(env.get('FILENAME')).sheet1

        return sheet

    def is_strike_current(self, task):
        cell = self.sheet.find(task).address
        #print(cell)
        result = gspread_formatting.get_effective_format(self.sheet, cell)

        return result.textFormat.strikethrough

    def cell_address(self, value):
        return self.sheet.find(value).address

    def share_spreadsheet(self):
        return self.sheet.share('main@mk-net.ru', perm_type='user', role='reader')

