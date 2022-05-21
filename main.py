import sqlite3
import requests
from bs4 import BeautifulSoup
import xmltodict


# 1)
import prtpy


def kns2sql():
    db = sqlite3.connect('my_db.db')
    cursor = db.cursor()
    cursor.execute("DROP TABLE IF EXISTS KNS_BILLNAME")

    table = """
        CREATE TABLE KNS_BILLNAME (
                BillNameID,
                BillID,
                Name,
                NameHistoryTypeID,
                NameHistoryTypeDesc,
                LastUpdatedDate
        );
    """
    cursor.execute(table)

    res = requests.get("http://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_BillName")  # get data base
    if res.status_code == 200:
        soup = BeautifulSoup(res.text, 'xml')        # parse xml response
        for entry in soup.find_all('properties'):    # reduce entries to relevant data
            data = xmltodict.parse(entry.__str__())  # parse raw data to dict
            results = []
            for key in data['m:properties'].keys():  # for every colum add the data to a list
                if type(data['m:properties'][key]) is type({}):
                    results.append(data['m:properties'][key]['#text'])
                else:
                    results.append(data['m:properties'][key])
            cursor.execute("""
                INSERT INTO KNS_BILLNAME(BillNameID,BillID,Name,NameHistoryTypeID,NameHistoryTypeDesc,LastUpdatedDate)
                 VALUES(?,?,?,?,?,?)
            """,results)   # update a row in the table

    else:
        print(res.status_code)

    db.commit()
    cursor.execute("""SELECT * FROM KNS_BILLNAME""")
    output = cursor.fetchall()
    print(output)
    db.close()


# 2)


import gspread
from prtpy import *


def run():
    account = gspread.service_account('credentials.json')  # connect to google sheets
    algo_input = account.open('input')
    output = account.open('output')
    raw = algo_input.worksheet('Sheet1').get_all_values()  # read raw data
    print(raw)
    data = [[int(y) for y in x if y != ''] for x in raw]   # convert data to int
    for item in data:                                      # run algorithm
        result = prtpy.partition(algorithm=partitioning.cbldm, numbins=2, outputtype=prtpy.out.Sums, items=item)
    output.worksheet("Sheet1").update("A1",abs(result[0] - result[1])) # update partition difference in the output sheet

