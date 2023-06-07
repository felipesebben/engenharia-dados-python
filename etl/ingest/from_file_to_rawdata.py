import pandas as pd
from google.cloud import storage
import yaml

if __name__ == "__main__":
    # ---- 1. alimentar as variaveis a partir do yaml ---- #
    with open(r"C:\Users\Felipe\OneDrive\OneDrive\Área de Trabalho\py-gcp\config\config.yaml", encoding="utf8") as file:
        config = yaml.safe_load(file)

    bucket_raw = config['bucket-rawdata']
    credentials_path = config['credentials_path']

    tabela = "products"

    # ---- 2. ler o arquivo local ---- #
    df = pd.read_csv("docs/products.csv", sep=";")
    
    # ---- 3. encaminhar para o storage ---- #
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