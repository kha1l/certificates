from oauth2client.service_account import ServiceAccountCredentials
import gspread
from configuration.config import Config
from datetime import datetime


def auto(name, df, dt):
    cfg = Config()
    scopes = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive',
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        './configuration/writer.json',
        scopes=scopes
    )
    date = datetime.strftime(dt, '%d.%m.%Y')
    gsc = gspread.authorize(credentials)
    sheet = gsc.open_by_key(cfg.table)
    gt = sheet.worksheet(name)
    gt.batch_clear(['B2:R100'])
    total = df.values.tolist()
    gt.update_cell(1, 1, date)
    params = {'valueInputOption': 'USER_ENTERED'}
    body = {'values': total}
    sheet.values_update(f'{name}!B2:P150', params, body)
