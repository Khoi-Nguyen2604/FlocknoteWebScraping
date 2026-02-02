from evaluatesql import start,insert_subscribe
from notescrapping import getposts
from statistic import getdata
import csv

# get people subscribe
def get_subscribe(csvfile):
    data=[]
    with open(csvfile,"r",encoding='windows-1252',newline='') as file:
        reader=csv.reader(file)
        for row in reader:
            print(row[:5])
            ID=row[0]
            name=row[1]+" "+row[2]
            phonenumber=row[4]
            email=row[3]
            data.append((ID,name,phonenumber,email))
    return data


if __name__=="__main__":
    file='Export-20250717-SaintAnnChurch.csv'
    cursor=start()
    for data in get_subscribe(file):
        insert_subscribe(cursor,*data)
    cursor.commit()
