import json
from datetime import datetime, date
import duckdb
import pandas as pd

# Date du jour au format "AAAA-MM-JJ"
today_date = datetime.now().strftime("%Y-%m-%d")

# Codes des villes
PARIS_CITY_CODE = 1
TOULOUSE_CITY_CODE = 2

def create_consolidate_tables():
    """
    Crée les tables consolidées à partir des fichiers SQL.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def consolidate_station_data_paris():
    """
    Consolidation des données de stations pour Paris.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    data = {}
    
    # Charger les données brutes JSON
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    # Normaliser les données JSON en DataFrame
    paris_raw_data_df = pd.json_normalize(data)
    
    # Ajouter des colonnes nécessaires
    paris_raw_data_df["id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
    paris_raw_data_df["address"] = None
    paris_raw_data_df["created_date"] = date.today()

    # Sélectionner et renommer les colonnes
    paris_station_data_df = paris_raw_data_df[[
        "id", "stationcode", "name", "nom_arrondissement_communes", "code_insee_commune",
        "address", "coordonnees_geo.lon", "coordonnees_geo.lat", "is_installed", 
        "created_date", "capacity"
    ]]
    paris_station_data_df.rename(columns={
        "stationcode": "code",
        "name": "name",
        "coordonnees_geo.lon": "longitude",
        "coordonnees_geo.lat": "latitude",
        "is_installed": "status",
        "nom_arrondissement_communes": "city_name",
        "code_insee_commune": "city_code"
    }, inplace=True)

    # Insérer les données dans la table consolidée
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM paris_station_data_df;")

def consolidate_city_data():
    """
    Consolidation des données des villes.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    data = {}

    # Charger les données JSON des villes
    with open(f"data/raw_data/{today_date}/city_data.json") as fd:
        data = json.load(fd)

    # Normaliser les données JSON en DataFrame
    raw_data_df = pd.json_normalize(data)

    # Sélectionner et renommer les colonnes
    city_data_df = raw_data_df[["code", "nom", "population"]]
    city_data_df.rename(columns={
        "code": "id",
        "nom": "name",
        "population": "nb_inhabitants"
    }, inplace=True)

    # Éliminer les doublons
    city_data_df.drop_duplicates(inplace=True)

    # Ajouter une colonne pour la date
    city_data_df["created_date"] = date.today()
    print(city_data_df)
    
    # Insérer les données dans la table consolidée
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")

def consolidate_station_statement_data_paris():
    """
    Consolidation des états des stations pour Paris.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    data = {}

    # Charger les données brutes JSON
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    # Normaliser les données JSON en DataFrame
    paris_raw_data_df = pd.json_normalize(data)
    
    # Ajouter des colonnes nécessaires
    paris_raw_data_df["station_id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
    paris_raw_data_df["created_date"] = date.today()

    # Sélectionner et renommer les colonnes
    paris_station_statement_data_df = paris_raw_data_df[[
        "station_id", "numdocksavailable", "numbikesavailable", "duedate", "created_date"
    ]]
    paris_station_statement_data_df.rename(columns={
        "numdocksavailable": "bicycle_docks_available",
        "numbikesavailable": "bicycle_available",
        "duedate": "last_statement_date",
    }, inplace=True)

    # Insérer les données dans la table consolidée
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM paris_station_statement_data_df;")

def consolidate_station_data_toulouse():
    """
    Consolidation des données de stations pour Toulouse.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    data = {}
    
    # Charger les données brutes JSON
    with open(f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    # Normaliser les données JSON en DataFrame
    toulouse_raw_data_df = pd.json_normalize(data)
    
    # Extraire les noms de villes uniques
    city_names = toulouse_raw_data_df["contract_name"].str.lower().unique()
    
    # Associer les noms des villes avec les codes INSEE
    city_to_insee = {}
    for city in city_names:
        city_insee = con.execute(
            "SELECT ID FROM CONSOLIDATE_CITY WHERE LOWER(NAME) = ?;",
            (city,)
        ).fetchone()
        if city_insee:
            city_to_insee[city] = city_insee[0]
        else:
            print(f"Attention : aucun code INSEE trouvé pour la ville {city}.")
    
    # Mapper les codes INSEE
    toulouse_raw_data_df["code_insee_commune"] = toulouse_raw_data_df["contract_name"].str.lower().map(city_to_insee)
    
    # Ajouter des colonnes nécessaires
    toulouse_raw_data_df["id"] = toulouse_raw_data_df["number"].apply(lambda x: f"{TOULOUSE_CITY_CODE}-{x}")
    toulouse_raw_data_df["created_date"] = date.today()
    toulouse_raw_data_df["status"] = toulouse_raw_data_df["status"].replace({"OPEN": "OUI", "CLOSE": "NON"})
    
    # Sélectionner et renommer les colonnes
    toulouse_station_data_df = toulouse_raw_data_df[[
        "id", "number", "name", "contract_name", "code_insee_commune", "address",
        "position.lon", "position.lat", "status", "created_date", "bike_stands"
    ]]
    toulouse_station_data_df.rename(columns={
        "number": "code",
        "position.lon": "longitude",
        "position.lat": "latitude",
        "contract_name": "city_name",
        "code_insee_commune": "city_code",
        "bike_stands": "capacity"
    }, inplace=True)

    # Insérer les données dans la table consolidée
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM toulouse_station_data_df;")

def consolidate_station_statement_data_toulouse():
    """
    Consolidation des états des stations pour Toulouse.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    data = {}

    # Charger les données brutes JSON
    with open(f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
        
    # Normaliser les données JSON en DataFrame
    toulouse_raw_data_df = pd.json_normalize(data)
    
    # Ajouter des colonnes nécessaires
    toulouse_raw_data_df["station_id"] = toulouse_raw_data_df["number"].apply(lambda x: f"{TOULOUSE_CITY_CODE}-{x}")
    toulouse_raw_data_df["created_date"] = date.today()

    # Sélectionner et renommer les colonnes
    toulouse_station_statement_data_df = toulouse_raw_data_df[[
        "station_id", "available_bike_stands", "bike_stands", "last_update", "created_date"
    ]]
    toulouse_station_statement_data_df.rename(columns={
        "available_bike_stands": "bicycle_docks_available",
        "bike_stands": "bicycle_available",
        "last_update": "last_statement_date",
    }, inplace=True)

    # Insérer les données dans la table consolidée
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM toulouse_station_statement_data_df;")
