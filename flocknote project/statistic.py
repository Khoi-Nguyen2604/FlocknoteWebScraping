import json
import os
from dotenv import load_dotenv
import cloudscraper
import time
from datetime import datetime
import notescrapping
load_dotenv()

#calculate metrics
def getdata(post):
    data={}
    data["date_collected"]=datetime.now().strftime("%Y-%m-%d")
    scraper=cloudscraper.create_scraper()
    url=f"https://stannphx.flocknote.com/note/{post['note_id']}/detailedAnalytics?ajax"
    headers=json.loads(os.getenv("FLOCKNOTE_HEADER"))
    # print(get_total_sent(url1,scraper,headers))
    opened=getopened(url,scraper,headers)
    data["opened"]=opened
    unopened=getunopened(url,scraper,headers)
    data["unopened"]=unopened
    sms=getsms(url,scraper,headers)
    data["sms"]=sms
    links=getlinks(url,scraper,headers)
    data["links"]=links
    unsubscribes=getunsubscribes(url,scraper,headers)
    data["unsubscribes"]=unsubscribes
    return data
    return response.json()["counts"]["total"]
def getopened(url,scraper,headers):
    openedlist=[]
    page=1
    i=0
    while True:
        payload={
        "type":"opened",
        "page": page
        }
        response=scraper.post(url,headers=headers,data=payload)
        if response.status_code!=200:
            print("No page found")
            break
        data=response.json()
        for record in data.get("records",[]):
            fullname=record["fname"]+" "+record["lname"]
            openedlist.append({"email":record["address"],"full name":fullname})
            i+=1
        if i>=data["totalRecords"]:
            break
        time.sleep(1)
        page+=1
    return openedlist

def getunopened(url,scraper,headers):
    unopenedlist=[]
    page=1
    i=0
    while True:
        payload={
        "type":"unopened",
        "page": page
        }
        response=scraper.post(url,headers=headers,data=payload)
        if response.status_code!=200:
            print("No page found")
            break
        data=response.json()
        for record in data.get("records",[]):
            fullname=record["fname"]+" "+record["lname"]
            unopenedlist.append({"email":record["address"],"full name":fullname})
            i+=1
        if i>=data["totalRecords"]:
            break
        time.sleep(1)
        page+=1
    return unopenedlist

def getsms(url,scraper,headers):
    smslist=[]
    page=1
    i=0
    while True:
        payload={
        "type":"sms",
        "page": page
        }
        response=scraper.post(url,headers=headers,data=payload)
        if response.status_code!=200:
            print("No page found")
            break
        data=response.json()
        for record in data.get("records",[]):
            fullname=record["fname"]+" "+record["lname"]
            smslist.append({"phone number":record["address"],"full name":fullname})
            i+=1
        if i>=data["totalRecords"]:
            break
        time.sleep(1)
        page+=1
    return smslist

def getlinks(url,scraper,headers):
    linklist=[]
    page=1
    payload={
        "type":"links",
        "page": page
        }
    response=scraper.post(url,headers=headers,data=payload)
    if response.status_code!=200:
        print("No page found")
        return linklist
    data=response.json()
    for record in data.get("records",[]):
        if isinstance(record,str):
            linklist.append({"url":data["records"][record]["url"],"count":data["records"][record]["cnt"]})
        else:
            linklist.append({"url":record["url"],"count":record["cnt"]})
    return linklist

def getunsubscribes(url,scraper,headers):
    unsubscribelist=[]
    page=1
    i=0
    while True:
        payload={
        "type":"unsubscribes",
        "page": page
        }
        response=scraper.post(url,headers=headers,data=payload)
        if response.status_code!=200:
            print("No page found")
            break
        data=response.json()
        for record in data.get("records",[]):
            fullname=record["fname"]+" "+record["lname"]
            unsubscribelist.append({"full name":fullname})
            i+=1
        if i>=data["totalRecords"]:
            break
        time.sleep(1)
        page+=1
    return unsubscribelist
if __name__=="__main__":    
    start_date="07-01-2025"
    end_date="07-15-2025"
    post=notescrapping.getposts(start_date,end_date)[0]
    data=getdata(post)
    print(data)