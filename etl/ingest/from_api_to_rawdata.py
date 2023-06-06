import requests
import pandas as pd
from google.cloud import storage
import yaml

def gettoken():
    """Pass user and password to get the token bearer."""
    url = "https://southamerica-east1-aula-gcp-arruda.cloudfunctions.net/api-for-class-get-customers"
    nome = "felipesebben"


    # corpo json da requisicao
    data = {
        "username": f"{nome}_aluno",
        "password": "aulapythonapi",
    }

    # enviar a req post com o corpo json
    response = requests.post(url, json=data)

    print(response.text)



if __name__ == "__main__":
    # ---- alimentar as variaveis a partir do yaml ---- #
    with open(r"C:\Users\Felipe\OneDrive\OneDrive\Área de Trabalho\py-gcp\config\config.yaml", encoding="utf8") as file:
        config = yaml.safe_load(file)
    
    bucket_raw = config['bucket-rawdata']
    credentials_path = config['credentials_path']

    tabela = "customers"

    # ---- 1. fazer o post para ober o token bearer ---- #
    gettoken()

