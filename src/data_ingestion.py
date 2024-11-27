import os
from datetime import datetime
import requests

def get_paris_realtime_bicycle_data():
    """
    Récupère les données en temps réel des vélos à Paris via une API et les sauvegarde.
    """
    # URL de l'API pour les données Velib' en temps réel
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
    
    # Effectuer une requête GET pour récupérer les données
    response = requests.request("GET", url)
    
    # Sauvegarder les données en JSON
    serialize_data(response.text, "paris_realtime_bicycle_data.json")

def serialize_data(raw_json: str, file_name: str):
    """
    Sauvegarde les données brutes JSON dans un fichier.
    """
    # Obtenir la date du jour au format "AAAA-MM-JJ"
    today_date = datetime.now().strftime("%Y-%m-%d")
    
    # Créer le répertoire s'il n'existe pas déjà
    if not os.path.exists(f"data/raw_data/{today_date}"):
        os.makedirs(f"data/raw_data/{today_date}")
    
    # Sauvegarder les données dans un fichier sous le répertoire du jour
    with open(f"data/raw_data/{today_date}/{file_name}", "w") as fd:
        fd.write(raw_json)

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
