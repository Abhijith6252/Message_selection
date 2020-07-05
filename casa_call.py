import psycopg2
import random
from sqlalchemy import create_engine
import pandas as pd
#to establish the connection with the DB
connection = psycopg2.connect(
    user="postgres", password="password", host="127.0.0.1", port="5432", database="CASA")
cursor = connection.cursor()

#to get the message list for the particular cmpg_id
def getmsgs(cmpg_id):
    cursor.execute(
        'SELECT "Messages"."msg_id" FROM "Messages" WHERE "Messages"."cmpg_id" =  %s;', [cmpg_id])
    msgs = cursor.fetchall()
    msg_id_list = [message for i in msgs for message in i]
    return msg_id_list


def Return_msg(customer_id, cmpg_id):
    cursor.execute(
        'SELECT *FROM "LookUp" WHERE "LookUp"."cmpg_id" = %s AND "LookUp"."customer_id"= %s;', (cmpg_id, customer_id))
    if len(cursor.fetchall()) < 10:
        msg_ids = []
        msg_ids = getmsgs(cmpg_id)
        return random.choice(msg_ids)

    else:

        # Getting the necessarry data as a pandas dataframe
        engine = create_engine(
            "postgresql://postgres:password@localhost:5432/CASA")
        df = pd.read_sql_query('select * from "LookUp"', con=engine)
        query_df = df.query('cmpg_id== @cmpg_id & customer_id== @customer_id ')
        # making them as dict
        msg_id_list = getmsgs(cmpg_id)
        msg_dict_count = dict.fromkeys(msg_id_list, 0)
        msg_dict_click = dict.fromkeys(msg_id_list, 0)
        msg_dict_buy = dict.fromkeys(msg_id_list, 0)
        # getting the count of the messages as a list
        count = dict(query_df["msg_id"].value_counts())
        for i in msg_dict_count:
            if i in count:
                msg_dict_count[i] = count[i]
        count_list = list(msg_dict_count.values())
        # getting the click as a list
        clicks = {}
        for i, j in query_df.iterrows():
            if j.msg_id not in clicks:
                clicks[j.msg_id] = int(j.click)
            else:
                clicks[j.msg_id] = clicks[j.msg_id] + int(j.click)

        for j in msg_dict_click:
            if j in clicks:
                msg_dict_click[j] = clicks[j]
        click_list = list(msg_dict_click.values())

        # getting the buy as a list
        buys = {}
        for i, j in query_df.iterrows():
            if j.msg_id not in buys:
                buys[j.msg_id] = int(j.buy)
            else:
                buys[j.msg_id] = buys[j.msg_id] + int(j.buy)

        for j in msg_dict_buy:
            if j in buys:
                msg_dict_buy[j] = buys[j]
        buy_list = list(msg_dict_buy.values())

        # calc rewards
        values = [0]*len(msg_id_list)
        for i in range(len(click_list)):
            values[i] = ((0.25*click_list[i])+(0.75*buy_list[i]))/count_list[i]
        print(values)

        # algorithm
        def explore():
            return random.choice(msg_id_list)

        def exploit():
            msg_index = values.index(max(values))
            return msg_id_list[msg_index]
        eps = 0.3

        def choose_message():
            if random.random() < eps:
                return explore()
            else:
                return exploit()
        return choose_message()
