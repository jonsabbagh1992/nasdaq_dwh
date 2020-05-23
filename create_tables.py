from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def initialize_database(conn):
    """
    - Establishes connection with the database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    """
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)