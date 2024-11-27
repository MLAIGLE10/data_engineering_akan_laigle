# Projet data engineer

Binome : Hanaa AKAN et Margaux LAIGLE

## Architecture du projet 

Nous avons 4 fichiers : 

- *data_ingestion* : fichier python pour récupérer et stocker les données dans des fichiers localement

- *data_consolidation* : fichier python pour consolider les données et faire un premier load dans une base de données type data-warehouse

- *data_agregation* : fichier python pour agréger les données et créer une modélisation de type dimensionnelle

- *main* : fichier python qui permet d'exécuter l'ensemble du code 

Le projet de base permet de récupérer les données de la ville de Paris, ses codes postaux ainsi que ses stations. Notre objectif étant d'étoffer la base de données en récupérant les informations de toutes les communes de France ainsi que les données des station de la ville de Toulouse.

Pour ce faire, nous avons modifié le code existant en ajoutant de nouvelles fonctions ou en les modifiant pour adapter le code existant à l'ajout de ces données. 

### 1. Ingestion des données

Premièrement, nous nous sommes concentrées sur l'ingestion des données. 

Nous avons, d'une part, récupéré les données des communes de France grâce à la fonction *get_city_data*. 

```python
def get_city_data():
    """
    Récupère les données des villes françaises via une API et les sauvegarde.
    """
    # URL de l'API pour les données des communes françaises
    url = "https://geo.api.gouv.fr/communes"
    
    # Effectuer une requête GET pour récupérer les données
    response = requests.request("GET", url)
    
    # Sauvegarder les données en JSON
    serialize_data(response.text, "city_data.json")
```

Cette fonction récupère les données des communes de l'api du gouvernement en ligne. Ainsi, on lui donne l'URL, on récupère les données et on les sauvegarde dans un fichier JSON. 

D'une autre part, nous avons récupéré les données des stations de la ville de Toulouse.

```python
def get_toulouse_realtime_bicycle_data():
    """
    Récupère les données en temps réel des vélos à Toulouse via une API et les sauvegarde.
    """
    # URL de l'API pour les données des vélos à Toulouse en temps réel
    url = "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/exports/json"
    
    # Effectuer une requête GET pour récupérer les données
    response = requests.request("GET", url)
    
    # Sauvegarder les données en JSON
    serialize_data(response.text, "toulouse_realtime_bicycle_data.json")
```

On procède de la même manière que pour les données des communes, en donnant une URL, en récupérant les données et les enregistrant dans un fichier JSON. 

### 2. Consolidation des données

Ensuite, nous sommes passées à la consolidation des données.

Premièrement, nous avons consolidé les données des communes :

```python
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
```

Ainsi, nous avons gardé les colonnes qui étaient déja présentes pour la ville de Paris. 

Ensuite, nous avons consolidé les données de Toulouse :

```python
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
```

```python
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
```


### 3. Agrégation des données

Au niveau de l'agrégation des données, nous n'avons pas eu besoin de réaliser quoique ce soit car le code existant était compatible avec nos nouvelles données. 


### 4. Le fichier main.py

Le fichier `main.py` contient le code principal du processus et exécute séquentiellement les différentes fonctions expliquées plus haut. 

```python

```

### En conclusion



### Comment faire fonctionner ce projet?

Pour faire fonctionner notre projet :

```bash 
git clone https://github.com/MLAIGLE10/data_engineering_akan_laigle.git

cd data_engineering_akan_laigle

python3 -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt

python src/main.py
```

### Analyse

Au final, vous devriez être capable de réaliser les requêtes SQL suivantes sur votre base de données DuckDB :

```sql
-- Nb d'emplacements disponibles de vélos dans une ville
SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
FROM DIM_CITY dm INNER JOIN (
    SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
    FROM FACT_STATION_STATEMENT
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
    GROUP BY CITY_ID
) tmp ON dm.ID = tmp.CITY_ID
WHERE lower(dm.NAME) in ('paris', 'nantes', 'vincennes', 'toulouse');

-- Nb de vélos disponibles en moyenne dans chaque station
SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
FROM DIM_STATION ds JOIN (
    SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available
    FROM FACT_STATION_STATEMENT
    GROUP BY station_id
) AS tmp ON ds.id = tmp.station_id;
```


