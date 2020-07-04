from scripts.sql_queries import create_table_queries_no_referential, create_table_queries_with_referential, drop_table_queries, index_queries

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn, referential):
    """
    Creates each table using the queries in `create_table_queries` list.
    
    """
    if referential:
        create_table_queries = create_table_queries_with_referential
    else:
        create_table_queries = create_table_queries_no_referential
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
def create_indices(cur, conn):
    for query in index_queries:
        cur.execute(query)
        conn.commit()

def initialize_database(conn, referential=False):
    """
    - Establishes connection with the database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    If referential is passed as True, the fact table will be created with referential integrity.
    It's passed as False by default because enforcing referential integrity massively slows down
    query performance when loading data into the fact table.
    """
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    create_tables(cur, conn, referential)
    create_indices(cur, conn)