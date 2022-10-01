import glob
import json
import os
from typing import List

from cassandra.cluster import Cluster


table_drop_event = "DROP TABLE event"
table_drop_repo = "DROP TABLE IF EXISTS Repo"

table_create_event = """
    CREATE TABLE IF NOT EXISTS event
    (
        id text,
        type text,
        public text,
        PRIMARY KEY (
            id,
            type
        )
    )
"""
table_create_repo = """
    CREATE TABLE IF NOT EXISTS Repo (
        id  text,
        name varchar,
        url varchar,
        PRIMARY KEY ((id), name)
        )
"""

create_table_queries = [
    table_create_event,table_create_repo
]
drop_table_queries = [
    table_drop_event,table_drop_repo
]

def drop_tables(session):
    for query in drop_table_queries:
        try:
            rows = session.execute(query)
        except Exception as e:
            print(e)


def create_tables(session):
    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)


def get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files


def process(session, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                # print(each["id"], each["type"], each["actor"]["login"])

                # Insert data into event tables
                query_event = "INSERT INTO Event (id,type,public) VALUES ('%s', '%s', '%s')" \
                        % (each["id"], each["type"], each["public"])
                session.execute(query_event)

                # Insert data into repo tables 
                query_repo = "INSERT INTO Repo (id,name,url) VALUES ('%s', '%s', '%s')" \
                         % (each["repo"]["id"], each["repo"]["name"], each["repo"]["url"])
                session.execute(query_repo)
        

#def insert_sample_data(session):
#    query = f"""
#    INSERT INTO events (id, type, public) VALUES ('23487929637', 'IssueCommentEvent', true)
#    """
#    session.execute(query)


def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    # Create keyspace
    try:
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS github_events
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
            """
        )
    except Exception as e:
        print(e)

    # Set keyspace
    try:
        session.set_keyspace("github_events")
    except Exception as e:
        print(e)

    drop_tables(session)
    create_tables(session)
    process(session, filepath="../data")
    
    #insert_sample_data(session)

    # Select data in Cassandra and print them to stdout
    query1 = """
    SELECT count(id) from Repo 
    """
    query2 = """
    SELECT * from event WHERE id='23488007821' 
     """
    try:
        rows = session.execute(query1)
    except Exception as e:
        print(e)

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()