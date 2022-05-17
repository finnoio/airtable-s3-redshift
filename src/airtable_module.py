import configparser
from pathlib import Path

import requests
import json


config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))
AIRTABLE_BASE_ID = config.get('AIRTABLE', 'BASE_ID')
AIRTABLE_API_KEY = config.get('AIRTABLE', 'API_KEY')

BASE_URL = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}'
HEADERS = {
    'Authorization': f'Bearer {AIRTABLE_API_KEY}',
    'Content-Type': 'application/json'
}


def get_events_by_dt_range(event_table: str, from_dt: str, to_dt: str) :
    formula = {'filterByFormula': f'AND(IS_AFTER(CREATED_AT, DATETIME_PARSE("{from_dt}T00:00:00Z")),'
                                  f'IS_BEFORE(CREATED_AT, DATETIME_PARSE("{to_dt}T00:00:00Z")))'}
    response = requests.get(f'{BASE_URL}/{event_table}', headers=HEADERS, params=formula).json()
    if response.get('error') == 'NOT_FOUND':
        raise ValueError(f'NOT_FOUND error occurred {BASE_URL}/{event_table}?filterByFormula={formula["filterByFormula"]}')
    return response


if __name__ == '__main__':
    with open('app.json', 'w') as f:
        json.dump(get_events_by_dt_range('App events', '2021-05-02', '2021-05-03'), f)



