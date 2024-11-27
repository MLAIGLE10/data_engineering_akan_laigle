import duckdb

def create_agregate_tables():
    """
    Crée les tables d'agrégation nécessaires à partir des scripts SQL.
    """
    # Connexion à la base de données DuckDB
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    
    # Ouvrir le fichier contenant les instructions SQL pour créer les tables d'agrégation
    with open("data/sql_statements/create_agregate_tables.sql") as fd:
        statements = fd.read()
        
        # Exécuter chaque instruction SQL séparée par un point-virgule
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def agregate_dim_station():
    """
    Remplit la table d'agrégation DIM_STATION avec les données consolidées des stations.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    
    # Instruction SQL pour insérer ou remplacer les données des stations
    sql_statement = """
    INSERT OR REPLACE INTO DIM_STATION
    SELECT 
        ID,
        CODE,
        NAME,
        ADDRESS,
        LONGITUDE,
        LATITUDE,
        STATUS,
        CAPACITTY
    FROM CONSOLIDATE_STATION
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION);
    """
    
    # Exécuter l'instruction SQL
    con.execute(sql_statement)

def agregate_dim_city():
    """
    Remplit la table d'agrégation DIM_CITY avec les données consolidées des villes.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    
    # Instruction SQL pour insérer ou remplacer les données des villes
    sql_statement = """
    INSERT OR REPLACE INTO DIM_CITY
    SELECT 
        ID,
        NAME,
        NB_INHABITANTS
    FROM CONSOLIDATE_CITY
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY);
    """
    
    # Exécuter l'instruction SQL
    con.execute(sql_statement)

def agregate_fact_station_statements():
    """
    Remplit la table FACT_STATION_STATEMENT avec les données consolidées des déclarations des stations.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # Instruction SQL pour insérer ou remplacer les données de facturation des stations
    sql_statement = """
    INSERT OR REPLACE INTO FACT_STATION_STATEMENT
    SELECT 
        STATION_ID, 
        cc.ID as CITY_ID, 
        BICYCLE_DOCKS_AVAILABLE, 
        BICYCLE_AVAILABLE, 
        LAST_STATEMENT_DATE, 
        current_date as CREATED_DATE
    FROM CONSOLIDATE_STATION_STATEMENT
    JOIN CONSOLIDATE_STATION 
        ON CONSOLIDATE_STATION.ID = CONSOLIDATE_STATION_STATEMENT.STATION_ID
    LEFT JOIN CONSOLIDATE_CITY as cc 
        ON cc.ID = CONSOLIDATE_STATION.CITY_CODE
    WHERE CITY_CODE != 0 
        AND CONSOLIDATE_STATION_STATEMENT.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION_STATEMENT)
        AND CONSOLIDATE_STATION.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
        AND cc.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY);
    """
    
    # Exécuter l'instruction SQL
    con.execute(sql_statement)
