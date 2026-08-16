import requests

def extract_dados_url(url: str):
    response = requests.get(url)
    response.raise_for_status()

    return response