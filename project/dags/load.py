import psycopg2

from airflow.decorators import task

@task
def load(dfs, db_params):
    """ Load data to Postgres database.

    Args:
        df (DataFrame): transformed data
        db_params (dict): parameters for Postgres connection
    
    """

    print('Start loading data...')

    # Connection to Postgres database
    connection = psycopg2.connect(database=db_params['POSTGRES_DB'],
                                  host=db_params['POSTGRES_HOST'],
                                  user=db_params['POSTGRES_USER'],
                                  password=db_params['POSTGRES_PASSWORD'],
                                  port=db_params['POSTGRES_PORT'])
    cursor = connection.cursor()

    # Create and fill addresses table
    query_create_table = "DROP TABLE IF EXISTS addresses CASCADE;\
    CREATE TABLE IF NOT EXISTS addresses (\
    id SERIAL PRIMARY KEY,\
    address VARCHAR(100) NOT NULL,\
    domanialite VARCHAR(50) NOT NULL,\
    arrondissement VARCHAR(50) NOT NULL,\
    address_complement VARCHAR(50) NOT NULL,\
    id_location VARCHAR(50) NOT NULL\
    );"
    cursor.execute(query_create_table)

    for index, row in dfs[0].iterrows():
        query_insert_value = f"INSERT INTO addresses (id, address,\
            domanialite, arrondissement, address_complement, id_location) VALUES\
            ('{index}', '{row[0]}', '{row[1]}', '{row[2]}', '{row[3]}', '{row[4]}')" 
        cursor.execute(query_insert_value)
    connection.commit()
    
    print('\n>> addresses table loaded!')
    
    # Check contents
    print('Content:')
    query_select = "SELECT * FROM addresses LIMIT 10"
    cursor.execute(query_select)
    print(*dfs[0].columns)
    for row in cursor.fetchall():
        print(*row)

    # Create and fill trees table
    query_create_table = "DROP TABLE IF EXISTS trees CASCADE;\
    CREATE TABLE IF NOT EXISTS trees (\
    id INTEGER PRIMARY KEY,\
    idbase INTEGER NOT NULL,\
    variety VARCHAR(50) NOT NULL,\
    family VARCHAR(50) NOT NULL,\
    species VARCHAR(50) NOT NULL,\
    circumference DECIMAL NOT NULL,\
    height DECIMAL NOT NULL,\
    development VARCHAR(50) NOT NULL,\
    planting_date DATE NOT NULL,\
    geo_point VARCHAR(50) NOT NULL,\
    id_address INTEGER REFERENCES addresses (id)\
    );"
    cursor.execute(query_create_table)

    for index, row in dfs[1].iterrows():
        query_insert_value = f"INSERT INTO trees (id, idbase, variety, family,\
            species, circumference, height, development, planting_date,\
            geo_point) VALUES\
            ('{index}', '{row[0]}', '{row[1]}', '{row[2]}', '{row[3]}', '{row[4]}',\
             '{row[5]}', '{row[6]}', '{row[7]}', '{row[8]}')" 
        cursor.execute(query_insert_value)
    connection.commit()
    
    print('\n>> trees table loaded!')
    
    # Check contents
    print('Content')
    query_select = "SELECT * FROM trees LIMIT 10"
    cursor.execute(query_select)
    print(*dfs[1].columns)
    for row in cursor.fetchall():
        print(*row)

    # Close connection to database
    cursor.close()
    connection.close()