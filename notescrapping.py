import cloudscraper 
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
from datetime import datetime
import time
import json
load_dotenv()
def getposts(start_date,end_date):
    scrapper=cloudscraper.create_scraper()
    start_date = int(datetime.strptime(start_date, "%m-%d-%Y").timestamp())
    end_date = int(datetime.strptime(end_date, "%m-%d-%Y").timestamp())
    posts=[]
    url="https://stannphx.flocknote.com/group/165683/notes"
    headers=json.loads(os.getenv("FLOCKNOTE_HEADER"))
    response=scrapper.get(url,headers=headers)
    if not(response.status_code==200):
        print("cant connect to flocknote")
    soup=BeautifulSoup(response.text,"html.parser")
    notes=soup.select("div.note_list_item")
    publishedDate=0 
    for note in notes:
        note_id = note.get("data-noteid")
        data_date = note.get("data-date")

        if note_id and data_date:
            publishedDate = int(data_date)
            if start_date <= publishedDate <= end_date:
                title = note.select_one(".title")
                summary = note.select_one(".summary")
                author = note.select_one(".author")
                posts.append({
                    "note_id": note_id,
                    "date": datetime.fromtimestamp(publishedDate).strftime("%Y-%m-%d"),
                    "title": title.get_text(strip=True) if title else "",
                    "summary": summary.get_text(strip=True) if summary else "",
                    "author": author.get_text(strip=True) if author else ""
                })
                time.sleep(1)

    # pagination step json response and activated with publish date
    while True:
        # get json after cloud scrapping
        base_url=f"https://stannphx.flocknote.com/group/165683/notes/{publishedDate}?ajax&more"
        response = scrapper.get(base_url, headers=headers)
        data=response.json()
        if response.status_code != 200:
            print(f"Failed to fetch more notes from {base_url}. Status code: {response.status_code}")
            break
        for note in data.get("notes",[]):
            note_id=note.get("ID")
            publishedDate=note.get("publishedDate")
            if note_id and publishedDate:
                publishedDate=int(publishedDate)
                if start_date<=publishedDate<=end_date:
                    title=note.get("title")
                    summary=note.get("summary")
                    author=note.get("author")
                    posts.append({
                        "note_id":note_id,
                        "date":datetime.fromtimestamp(publishedDate).strftime("%Y-%m-%d"),
                        "title":title,
                        "summary":summary,
                        "author":author
                    })
                    time.sleep(1)
                else:
                    break
        if not data["more"] or not(start_date<=publishedDate<=end_date):
            break
 
    return posts

if __name__=="__main__":
    start_date="05-01-2025"
    end_date="07-15-2025"
    print(getposts(start_date,end_date))