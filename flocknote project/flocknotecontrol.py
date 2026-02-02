import statistic
import evaluatesql
import notescrapping
import cloudscraper 
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
from datetime import datetime
import time
import json
load_dotenv()

#main workflow for the database
def updatedata(start_date,end_date):
    cursor=evaluatesql.start()
    posts=notescrapping.getposts(start_date,end_date)
    for post in posts:
        data=statistic.getdata(post)
        collectiondate=data["date_collected"]
        datestamp=post["date"]
        MessagesSent=post["title"]
        evaluatesql.insert_message_sent(cursor,datestamp,collectiondate,MessagesSent)
        links=data["links"]
        print(links)
        for link in links:
            if link:
                evaluatesql.insert_links(cursor,datestamp,link["url"],MessagesSent,link["count"])
        unsubscribes=data["unsubscribes"]
        for unsubscribe in unsubscribes:
            if unsubscribe:
                evaluatesql.insert_unsubscribes(cursor,datestamp,MessagesSent,unsubscribe["full name"])
        sms=data["sms"]
        for message in sms:
            if message:
                evaluatesql.insert_sms(cursor,datestamp,MessagesSent,message["full name"],message["phone number"])
        for mess in data["unopened"]:
            if mess:
                evaluatesql.insert_unopened(cursor,datestamp,MessagesSent,mess["full name"],mess["email"])
        for mess in data["opened"]:
            if mess:
                evaluatesql.insert_opened(cursor,datestamp,MessagesSent,mess["full name"],mess["email"])
    cursor.commit()

if __name__=="__main__":
    start_date="01-1-2025"
    end_date="07-17-2025"
    updatedata(start_date,end_date)