import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.errors import HttpError
# ^ Google ^
import os.path
# ^ Check creds ^
import requests
# ^ Take usd/rub ^
import psycopg2
from datetime import datetime
# ^ Use database ^
from time import sleep
# ^ Delay ^
from pprint import pprint
from config import CREDENTIALS_FILE, api, version, spreadsheet_id, majorDimension, range, table, host, user, password, dbname, DELAY
# ^ Comfort ^


"""
Check access
Connection
Get data in range
Return values
"""
def get_sheet():
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

    if not os.path.exists(CREDENTIALS_FILE):
        print('[ERROR]\t creds.json not found')
        exit()
    else:
        try:
            credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPES)
            httpAuth = credentials.authorize(httplib2.Http())
            service = apiclient.discovery.build(api, version, http = httpAuth)
        except ValueError as err:
            print('[ERROR]\t invalid token')
        try:
            sheet = service.spreadsheets().values().get(
                spreadsheetId = spreadsheet_id,
                range = range,
                majorDimension = majorDimension).execute()

        except HttpError as err:
            print(err)

        return sheet['values']


"""
Request to CBR and return usd/rub
"""
def get_usd():
    usd_raw = requests.get('https://www.cbr-xml-daily.ru/daily_json.js').json()
    usd = usd_raw['Valute']['USD']['Value']
    return usd


"""
Edit table from google
Add row usd*(usd/rub) //price in rub

"""
def add_rub(sheet, USD):
    result = list(map(lambda x: (
            int(x[0]),
            int(x[1]),
            float(x[2]),
            round(float(x[2]) * USD, 2), #price in rub
            '-'.join(x[3].split('.')[::-1])), #format date for database
        sheet))

    return result


"""
Generate SQL Requests
"table" from config.py
"""
def gen_sel():
    sql = "select * from {}".format(table)
    return sql


def gen_del(data):
    sql = ''

    if data:
        for order in data:
            sql += "delete from {} where order_num ={};\n".format(table, order[1])

    return sql


def gen_upd(data):
    sql = ''

    if data:
        for order in data:
            sql += "update {} set num={}, price_usd={}, price_rub={}, date='{}' where order_num ={};\n".format(table, order[0],order[2],order[3],order[4],order[1])

    return(sql)


def gen_ins(data):
    if data:
        insert = "insert into {}(num, order_num, price_usd, price_rub, date) values".format(table)

        list_of_values = list(map(lambda x: str(x).replace(']',')').replace('[','('),data))
        values = str(','.join(list_of_values))

        res = ''
        for string in list_of_values:
            res += string + ','
        sql = insert + res[:-1]

        return sql

    else: return ''


"""
Connect to database
Execute SQL request
If data to fetch -> return data
Else nothing                    //may be better return None
"""
def sql_exec(sql):
    print(sql)
    try:
        result = None
        connection = None

        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            dbname=dbname
        )
        connection.autocommit = True

        with connection.cursor() as cursor:
            cursor.execute(
                sql
            )
            if cursor:
                result = cursor.fetchall()

    except Exception as e:
        pass
    finally:
        if connection:
            connection.close()
            if result:
                return result


"""
Optimization of SQL Request
Separation data on 4 groups
1 - data to update      //to_upd
2 - data to delete      //to_del
3 - data to insert      //to_ins
4 - pass
"""
def separete_orders(db_data, sheet_data):
    to_upd = []
    to_del = []
    to_ins = []
    if(db_data and sheet_data):
        rm_db_data = set() #with this sets we clear duplicates from DB and Sheets data
        rm_sh_data = set()

        converted_data = list(map(lambda x: (
            x[0],
            x[1],
            x[2],
            x[3],
            x[4].strftime("%Y-%m-%d")), #from DB we get datetime object. Convert it to string for compare
            db_data))

        for order in converted_data:
            for sheet_row in sheet_data:
                if (sheet_row[1] == order[1]):
                    if (order != sheet_row):
                        to_upd.append(sheet_row)

                    rm_db_data.add(order)
                    rm_sh_data.add(sheet_row)

        #remove duplicates
        to_del = set(converted_data).difference(rm_db_data)
        to_ins = set(sheet_data).difference(rm_sh_data)

    return(to_upd, to_del, to_ins)


"""
Get data from Google sheets
Chek hash
If data edited:
    Get data from database
    Optimize SQL request
    Update/Delete/Insert data in database
Else:
    Sleep and request again
"""
def main():
    prev_hash = 0

    while True:
        sheet = get_sheet()
        sheet_for_hash = tuple(map(lambda x: tuple(x), sheet)) # hash(list) - impossible | hash(tuple) - possible
        USD = get_usd()

        new_hash = hash((sheet_for_hash, USD))
        if prev_hash != new_hash:
            prev_hash = new_hash # update hash
            sheet_rub = add_rub(sheet, USD)
            sql_sel = gen_sel()
            db_data = sql_exec(sql_sel)
            to_req = separete_orders(db_data, sheet_rub)

            # generate SQL
            sql_upd = gen_upd(to_req[0])
            sql_del = gen_del(to_req[1])
            sql_ins = gen_ins(to_req[2])

            sql = sql_del+sql_upd+sql_ins

            sql_exec(sql)

        sleep(DELAY)


if __name__ == "__main__":
    main()
