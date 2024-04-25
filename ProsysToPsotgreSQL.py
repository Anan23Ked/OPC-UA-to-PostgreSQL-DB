from  opcua import Client
import psycopg2
from datetime import datetime
import time

client = Client("opc.tcp://SMPM-37:53530/OPCUA/SimulationServer")
db = {
        "database" : "test_db",
        "user":"postgres",
        "password":"1234",
        "host":"localhost",
        "port":"5432"
}

def conn_postgres():
    try:

        
        conn = psycopg2.connect(**db)
        cursor = conn.cursor()
        return conn,  cursor
    except Exception as e:
        print(f"Failed to connect to db {e}")
        return None, None


def connect_to_opc_ua_server():
    try:
        client.connect()
        print("Connected to OPC UA Server")
        return client
    except Exception as e:
        print(f"Failed to connect to OPC UA Server: {e}")
        return None

def insert(conn, cursor):
    try:

        Counter = client.get_node("ns=3;i=1002").get_value()
        Square = client.get_node("ns=3;i=1006").get_value()
        Triangle = client.get_node("ns=3;i=1007").get_value()
        currenttime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("INSERT INTO prosys (currenttime, Counter, Square, Triangle) VALUES (%s, %s, %s, %s)", 
                    (currenttime, Counter, Square, Triangle))
        conn.commit()

        print("Data inserted into PostgreSQL")
        time.sleep(1)
    except Exception as e:
        print(f"Failed to insert data into PostgreSQL: {e}")
        
def main():
    opc_client = connect_to_opc_ua_server()
    db_conn, db_cursor = conn_postgres()

    if opc_client is None or db_conn is None:
        return
    while True:
        try:
            insert(db_conn, db_cursor)
        except Exception as e:
            print(f"Error fetching data: {e}")

if __name__ == "__main__":
    main()
