from google.cloud import storage
import yaml

def rodar(dict_objetos_finais):
    # ---- 1. alimentar as variáveis a partir do yaml ---- #
    with open(r"C:\Users\Felipe\OneDrive\OneDrive\Área de Trabalho\py-gcp\config\config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    
    bucket_dw = config["bucket-negocio"]
    credentials_path = config["credentials_path"]

    # ---- 2. cria o client do storage ---- #
    client_storage = storage.Client.from_service_account_json(credentials_path)
    bucket = client_storage.bucket(bucket_dw)

    for tabela in dict_objetos_finais:
        try:
            df = dict_objetos_finais[tabela]["df_final"]

            # ---- 3. criar arquivo CSV com o DataFrame ---- #
            csv_data = df.to_csv(index=False, encoding="utf-8-sig", sep=";")
            csv_data = csv_data.encode("iso-8859-1")

            # ---- 4. carregar o arquivo para o Cloud Storage ---- #
            bucket_path = f"silver/{tabela}/{tabela}.csv"
            blob = bucket.blob(bucket_path)
            blob.upload_from_string(csv_data, content_type="csv")
        except Exception as e:
            print("Houve erro aqui")
            return {"deve-rodar": False,
                    "description": e,
                    }
    return {"deve-rodar": True,
            "description": False,
            }