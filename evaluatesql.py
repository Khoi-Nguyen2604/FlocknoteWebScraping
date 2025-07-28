import sqlite3
import notescrapping
import statistic
def start():
    cursor=sqlite3.connect("flocknote.db")
    return cursor

def test():
    cursor=sqlite3.connect("test.db")
    return cursor

def insert_message_sent(cursor,datestamp,collectiondate,MessagesSent):
    cursor.execute("""INSERT OR IGNORE INTO flocknote_MessagesSent VALUES(?,?,?);""",(datestamp,MessagesSent,collectiondate))

def insert_links(cursor,datestamp,link,MessageSent,counts):
    cursor.execute("""INSERT OR IGNORE INTO flocknote_links VALUES(?,?,?,?);""",(datestamp,MessageSent,link,counts))

def insert_unsubscribes(cursor,datestamp,MessagesSent,name):
    cursor.execute("""INSERT OR IGNORE INTO flocknote_unsubscribes VALUES(?,?,?);""",(datestamp,MessagesSent,name))

def insert_sms(cursor,datestamp,MessageSent,name,phonenumber):
    cursor.execute("""INSERT OR IGNORE INTO flocknote_sms VALUES(?,?,?,?);""",(datestamp,MessageSent,name,phonenumber))

def insert_unopened(cursor,datestamp,MessagesSent,name,email):
    cursor.execute("""INSERT OR IGNORE INTO flocknote_messageAction VALUES(?,?,?,?,?);""",(datestamp,False,MessagesSent,name,email))

def insert_opened(cursor,datestamp,MessagesSent,name,email):
    cursor.execute("""INSERT OR IGNORE INTO flocknote_messageAction VALUES(?,?,?,?,?);""",(datestamp,True,MessagesSent,name,email))

def insert_subscribe(cursor,ID,name,phonenumber,email):
    cursor.execute("""INSERT OR IGNORE INTO flocknote_subscribes VALUES(?,?,?,?);""",(ID,name,phonenumber,email))

if __name__=="__main__":
    #test
    cursor=test()
    start_date="07-1-2025"
    end_date="07-17-2025"
    post=notescrapping.getposts(start_date,end_date)[0]
    data=statistic.getdata(post)
    collectiondate=data["date_collected"]
    datestamp=post["date"]
    MessagesSent=post["title"]
    insert_message_sent(cursor,datestamp,collectiondate,MessagesSent)
    links=data["links"]
    for link in links:
        if link:
            insert_links(cursor,datestamp,link["url"],MessagesSent,link["count"])
    unsubscribes=data["unsubscribes"]
    for unsubscribe in unsubscribes:
        if unsubscribe:
            insert_unsubscribes(cursor,datestamp,MessagesSent,unsubscribe["full name"])
    sms=data["sms"]
    for message in sms:
        if message:
            insert_sms(cursor,datestamp,MessagesSent,message["full name"],message["phone number"])
    for mess in data["unopened"]:
        if mess:
            insert_unopened(cursor,datestamp,mess["full name"],mess["email"])
    for mess in data["opened"]:
        if mess:
            insert_opened(cursor,datestamp,mess["full name"],mess["email"])
    cursor.commit()
    
    
    
    
    
        
    