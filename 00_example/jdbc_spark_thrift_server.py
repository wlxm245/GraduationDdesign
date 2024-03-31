from pyhive import hive

if __name__ == '__main__':
    conn = hive.Connection(host="node1", port=10000, username="hadoopadmin")

    cursor = conn.cursor( )

    cursor.execute("show tables")

    result = cursor.fetchall()

    print(result)