import pandas as pd
import io
from google.cloud import storage
import yaml
import dask.dataframe as dd


def rodar():
    # ---- 1. alimentar as variaveis a partir do yaml ---- #
    with open(r"C:\Users\Felipe\OneDrive\OneDrive\Área de Trabalho\py-gcp\config\config.yaml", encoding="utf8") as file:
        config = yaml.safe_load(file)

    bucket_raw = config['bucket-rawdata']
    credentials_path = config['credentials_path']
    bucket_processed = config["bucket-processado"]

    # ---- 2. capturar os DataFrames ---- #
    parent_folder = "oltp-north" # primeira pasta do rawdata
    objetos = {
        "df_orders": {"pasta": "orders", "df": None}, # nomear DataFrame - 1o item pasta e 2o, df vazio
        "df_order_details": {"pasta": "order_details", "df": None},
        "df_products": {"pasta": "products", "df": None},
        "df_employees": {"pasta": "employees", "df": None},
        "df_customers": {"pasta": "customers", "df": None},
        "df_shippers": {"pasta": "shippers", "df": None},
        "df_categories": {"pasta": "categories", "df": None},
        "df_suppliers": {"pasta": "suppliers", "df": None},
    }

    list_dfs_executados = []

    # ---- 3. criar lista de arquivos que ja rodaram, ou seja, legado/processados
    try:
        df_arquivos_legado = dd.read_csv(f"gs://{bucket_processed}/from-rawdata-to-silver/*", encoding="iso-8859-1",
                                         sep=";") # ler todos os arquivos do diretorio com desk
        df_arquivos_legado = df_arquivos_legado.compute() # converter objeto dask para DataFrame
        df_arquivos_legado.rename(columns={df_arquivos_legado[0]: "arquivo"}, inplace=True) # corrigir erro de nomeação de "arquivo"
        
    except:
        df_arquivos_legado = pd.DataFrame({"arquivo": None}, index=[0])

    list_legado = df_arquivos_legado["arquivo"].to_list()
    
    # ---- 4. iniciar loop para cada item do nosso dicionario ---- #
    for i in objetos:
        pasta = objetos[i]["pasta"]
        client_storage = storage.Client.from_service_account_json(credentials_path)
        
        bucket = client_storage.get_bucket(bucket_raw)
        blob_list = list(bucket.list_blobs(prefix=f"{parent_folder}/{pasta}")) # filtrar arquivos em que o prefixo do arquivo tenha o parent folder + nome pasta

        list_dfs = [] # lista de DataFrames

        for blob in blob_list:
            id_file = f"{blob.name}_{blob.updated.strftime('%Y%m%d%H%M%S')}" # criar id baseado no nome do blob + timestamp de ultima atualizacao
            if "orders" in blob.name or "order_details" in blob.name: # tabelas- fato - checar se houve alteracoes
                if not id_file in list_legado: # checar se id_file existe na lista de legados
                    print(f"Arquivo {blob.name} será processado para silver pois é novo.")
                    list_dfs_executados.append(pd.DataFrame({"arquivo": id_file}, index=[0])) # popular lista com arquivos processados

                else:
                    print(f"Arquivo {blob.name} não será processado para silver pois já foi alimentado no dw anteriormente.")

                    return {"deve-rodar": False}
            
            file_bytes = blob.download_as_bytes() # tudo que nao for orders, fazer download de memoria
            file_buffer = io.BytesIO(file_bytes) # transformar em buffer para nao sobrecarregar memoria
            df = pd.read_csv(file_buffer, encoding='iso-8859-1', sep=";")

            list_dfs.append(df)

        # ---- 5. empilhar os DataFrames lidos do rawdata ---- #
        combined_df = pd.concat(list_dfs, ignore_index=True)
        objetos[i]["df"] = combined_df

    
    # ---- 6. empilhar os DataFrames dos nomes de arquivos ---- #
    if len(list_dfs_executados) > 0:
        df_arquivos_lidos = pd.concat(list_dfs_executados, ignore_index=True)
    
    return {"deve-rodar": True,
            "objetos": objetos,
            "df_arquivos_runtime": df_arquivos_lidos,
    }


if __name__ == "__main__":
    rodar()