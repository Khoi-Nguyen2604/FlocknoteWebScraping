from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import notescrapping
import statistic
#check connection with big query
def check_bigquery_table_status(project_id, dataset_id, table_id):
    """Checks if the specified BigQuery table exists and is accessible."""
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        table = client.get_table(table_ref)  # API request
        print(f"Table {table_id} found. Table description: {table.description}")
        return True
    except NotFound:
        print(f"Table {table_id} not found or inaccessible.")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

#start the project 
def start():
    project = "rich-synapse-465312-h7"
    dataset = "flocknotedatabase"
    return project,dataset

def _run_merge(client: bigquery.Client, sql: str, params: list[bigquery.ScalarQueryParameter]):
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    client.query(sql, job_config=job_config).result()
    print("Upsert complete")

# Example usage:
# Replace with your project, dataset, and table details
def upsert_message_sent(PROJECT, DATASET, datestamp, CollectionDateStamp, MessageSent,TABLE="flocknote_MessagesSent"):
    client = bigquery.Client(project=PROJECT)

    sql = f"""
    MERGE `{PROJECT}.{DATASET}.{TABLE}` T
    USING (
      SELECT
        @datestamp AS datestamp,
        @CollectionDateStamp AS CollectionDateStamp,
        @MessageSent AS MessageSent
    ) S
    ON T.datestamp = S.datestamp
    WHEN MATCHED THEN
      UPDATE SET
        T.MessageSent = S.MessageSent,
        T.CollectionDateStamp = S.CollectionDateStamp
    WHEN NOT MATCHED THEN
      INSERT (datestamp, CollectionDateStamp, MessageSent)
      VALUES (S.datestamp, S.CollectionDateStamp, S.MessageSent)
    """
    _run_merge(client, sql, [
        bigquery.ScalarQueryParameter("datestamp", "DATE", datestamp),
        bigquery.ScalarQueryParameter("CollectionDateStamp", "DATE", CollectionDateStamp),
        bigquery.ScalarQueryParameter("MessageSent", "STRING", MessageSent)
    ])

def upsert_links(PROJECT, DATASET, datestamp, MessageSent, link, counts, TABLE="flocknote_Links",):
    client = bigquery.Client(project=PROJECT)
    sql = f"""
    MERGE `{PROJECT}.{DATASET}.{TABLE}` T
    USING (
      SELECT
        @datestamp AS datestamp,
        @MessageSent AS MessageSent,
        @link AS link,
        @counts AS counts
    ) S
    ON T.datestamp = S.datestamp
       AND T.MessageSent = S.MessageSent
       AND T.link = S.link
    WHEN MATCHED THEN
      UPDATE SET
        T.counts = S.counts
    WHEN NOT MATCHED THEN
      INSERT (datestamp, MessageSent, link, counts)
      VALUES (S.datestamp, S.MessageSent, S.link, S.counts)
    """
    _run_merge(client, sql, [
        bigquery.ScalarQueryParameter("datestamp", "DATE", datestamp),
        bigquery.ScalarQueryParameter("MessageSent", "STRING", MessageSent),
        bigquery.ScalarQueryParameter("link", "STRING", link),
        bigquery.ScalarQueryParameter("counts", "INT64", counts),
    ])


def upsert_unsubscribes(PROJECT, DATASET, datestamp, MessageSent, name, TABLE="flocknote_Unsubscribes"):
    client = bigquery.Client(project=PROJECT)
    sql = f"""
    MERGE `{PROJECT}.{DATASET}.{TABLE}` T
    USING (
      SELECT
        @datestamp AS datestamp,
        @MessageSent AS MessageSent,
        @name AS name
    ) S
    ON T.datestamp = S.datestamp
       AND T.MessageSent = S.MessageSent
       AND T.name = S.name
    WHEN MATCHED THEN
      UPDATE SET
        T.name = S.name
    WHEN NOT MATCHED THEN
      INSERT (datestamp, MessageSent, name)
      VALUES (S.datestamp, S.MessageSent, S.name)
    """
    _run_merge(client, sql, [
        bigquery.ScalarQueryParameter("datestamp", "DATE", datestamp),
        bigquery.ScalarQueryParameter("MessageSent", "STRING", MessageSent),
        bigquery.ScalarQueryParameter("name", "STRING", name),
    ])


def upsert_sms(PROJECT, DATASET, datestamp, MessageSent, name, phone_number, TABLE="flocknote_Sms"):
    client = bigquery.Client(project=PROJECT)
    sql = f"""
    MERGE `{PROJECT}.{DATASET}.{TABLE}` T
    USING (
      SELECT
        @datestamp AS datestamp,
        @MessageSent AS MessageSent,
        @name AS name,
        @phone_number AS phone_number
    ) S
    ON T.datestamp = S.datestamp
       AND T.MessageSent = S.MessageSent
       AND T.phone_number = S.phone_number
    WHEN MATCHED THEN
      UPDATE SET
        T.name = S.name
    WHEN NOT MATCHED THEN
      INSERT (datestamp, MessageSent, name, phone_number)
      VALUES (S.datestamp, S.MessageSent, S.name, S.phone_number)
    """
    _run_merge(client, sql, [
        bigquery.ScalarQueryParameter("datestamp", "DATE", datestamp),
        bigquery.ScalarQueryParameter("MessageSent", "STRING", MessageSent),
        bigquery.ScalarQueryParameter("name", "STRING", name),
        bigquery.ScalarQueryParameter("phone_number", "STRING", phone_number),
    ])

def upsert_message_action(PROJECT,DATASET,datestamp,is_opened,MessageSent,name,email,TABLE="flocknote_MessageAction"):
    client = bigquery.Client(project=PROJECT)

    sql = f"""
    MERGE `{PROJECT}.{DATASET}.{TABLE}` T
    USING (
      SELECT
        @datestamp AS datestamp,
        @is_opened AS is_opened,
        @MessageSent AS MessageSent,
        @name AS name,
        @email AS email
    ) S
    ON T.datestamp = S.datestamp
       AND T.MessageSent = S.MessageSent
       AND T.email = S.email
    WHEN MATCHED THEN
      UPDATE SET
        T.is_opened = S.is_opened,
        T.name = S.name
    WHEN NOT MATCHED THEN
      INSERT (datestamp, is_opened, MessageSent, name, email)
      VALUES (S.datestamp, S.is_opened, S.MessageSent, S.name, S.email)
    """
    _run_merge(client, sql, [
        bigquery.ScalarQueryParameter("datestamp", "DATE", datestamp),
        bigquery.ScalarQueryParameter("is_opened", "INTEGER", is_opened),
        bigquery.ScalarQueryParameter("MessageSent", "STRING", MessageSent),
        bigquery.ScalarQueryParameter("name", "STRING", name),
        bigquery.ScalarQueryParameter("email", "STRING", email),
    ])

def upsert_unopened(PROJECT, DATASET, datestamp, MessageSent, name, email):
    upsert_message_action(PROJECT, DATASET, TABLE, datestamp, False, MessageSent, name, email)

def upsert_opened(PROJECT, DATASET, datestamp, MessageSent, name, email):
    upsert_message_action(PROJECT, DATASET, TABLE, datestamp, True, MessageSent, name, email)


def upsert_subscribe(PROJECT, DATASET, ID, name, phonenumber, email, TABLE="flocknote_Subscribes"):
    client = bigquery.Client(project=PROJECT)
    sql = f"""
    MERGE `{PROJECT}.{DATASET}.{TABLE}` T
    USING (
      SELECT
        @ID AS ID,
        @name AS name,
        @phonenumber AS phonenumber,
        @email AS email
    ) S
    ON T.ID = S.ID
    WHEN MATCHED THEN
      UPDATE SET
        T.name = S.name,
        T.phonenumber = S.phonenumber,
        T.email = S.email
    WHEN NOT MATCHED THEN
      INSERT (ID, name, phonenumber, email)
      VALUES (S.ID, S.name, S.phonenumber, S.email)
    """
    _run_merge(client, sql, [
        bigquery.ScalarQueryParameter("ID", "STRING", str(ID)),
        bigquery.ScalarQueryParameter("name", "STRING", name),
        bigquery.ScalarQueryParameter("phonenumber", "STRING", phonenumber),
        bigquery.ScalarQueryParameter("email", "STRING", email),
    ])



#workflow using googlecloudapi
#main workflow for the database
def updatedata(start_date,end_date):
    (project,dataset)=start()
    posts=notescrapping.getposts(start_date,end_date)
    for post in posts:
        data=statistic.getdata(post)
        collectiondate=data["date_collected"]
        datestamp=post["date"]
        MessagesSent=post["title"]
        upsert_message_sent(project,dataset,datestamp,collectiondate,MessagesSent)
        links=data["links"]
        print(links)
        for link in links:
            if link:
                upsert_links(project,dataset,datestamp,MessagesSent,link["url"],link["count"])
        unsubscribes=data["unsubscribes"]
        for unsubscribe in unsubscribes:
            if unsubscribe:
                upsert_unsubscribes(project,dataset,datestamp,MessagesSent,unsubscribe["full name"])
        sms=data["sms"]
        for message in sms:
            if message:
                upsert_sms(project,dataset,datestamp,MessagesSent,message["full name"],message["phone number"])
        for mess in data["unopened"]:
            if mess:
                upsert_unopened(project,dataset,datestamp,MessagesSent,mess["full name"],mess["email"])
        for mess in data["opened"]:
            if mess:
                upsert_opened(project,dataset,datestamp,MessagesSent,mess["full name"],mess["email"])
    cursor.commit()


if __name__=="__main__":
    (project,dataset)=start()
    table="flocknote_Links"
    if check_bigquery_table_status(project, dataset, table):
        print("Connection and table access successful.")
    else:
        print("Table check failed.")
    start_date="01-01-2026"
    end_date="01-20-2026"
    updatedata(start_date,end_date)
    
