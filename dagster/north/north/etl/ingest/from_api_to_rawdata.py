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

    # armazenar a resposta
    result = response.json()
    token = result["bearer"]
    
    return token # retornar o token ao executar funcao



# if __name__ == "__main__":
def rodar():
    # ---- alimentar as variaveis a partir do yaml ---- #
    with open(r"C:\Users\Felipe\OneDrive\OneDrive\Área de Trabalho\py-gcp\config\config.yaml", encoding="utf8") as file:
        config = yaml.safe_load(file)
    
    bucket_raw = config['bucket-rawdata']
    credentials_path = config['credentials_path']

    tabela = "customers"

    # ---- 1. executar o POST para ober o token bearer ---- #
    token = gettoken()
    print(token)

    # ---- 2. executar GET para obter resultado do dado ---- #
    header = {"token": f"Bearer {token}"}
    
    primeira_pagina = 1
    url = f"https://southamerica-east1-aula-gcp-arruda.cloudfunctions.net/api-for-class-get-customers?page={primeira_pagina}"

    response = requests.get(url, headers=header)
    
    data = response.json()["data"] # armazenar os dados
    df = pd.DataFrame(data)

    # ---- 3. obter o total de paginas ---- #
    total_paginas = int(response.json()["admpages"]["total_pages"])
    
    # ---- 4. obter lista de DataFrames ---- #
    lista_de_dfs = [df]


    for pagina in range(2, total_paginas + 1):
        print(f"{pagina} de {total_paginas}")
        url = f"https://southamerica-east1-aula-gcp-arruda.cloudfunctions.net/api-for-class-get-customers?page={pagina}"

        response = requests.get(url, headers=header)
        data = response.json()["data"]
        df = pd.DataFrame(data)
        lista_de_dfs.append(df)
    
    # ---- 5. concatenar lista de DataFrames em um unico df ---- #
    df_final = pd.concat(lista_de_dfs, ignore_index=True)

    df = df_final.copy() # fazer alteracoes em uma copia

    # ---- 6. etapa de ingestao para o storage ---- #
    client_storage = storage.Client.from_service_account_json(credentials_path)

    if df.shape[0] > 0:
        folder_path = f"oltp-north/{tabela}_x/{tabela}.csv"
        bucket = client_storage.bucket(bucket_raw)

        # cria arquivo csv com o DataFrame
        csv_data = df.to_csv(index=False, encoding="utf-8-sig", sep=";")
        csv_data = csv_data.encode("iso-8859-1") # encode considerando caracteres latinos

        # upload para o storage
        blob = bucket.blob(folder_path) # criar id para o path indicado
        blob.upload_from_string(csv_data)
    
        print(f"Tabela {tabela} carregada no destino {folder_path}")
        
    else:
        print(f"Tabela {tabela} vazia, portanto, não carregada para o storage")