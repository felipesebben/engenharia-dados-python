import yaml
from dw import dict_modelo
import gcsfs
from google.cloud import bigquery
from google.cloud import storage
from datetime import timedelta
import pandas as pd
import io
import dask.dataframe as dd



# if __name__ == "__main__":
def rodar():
    # ---- 1. alimentar as variáveis a partir do yaml ---- #
    with open(r"C:\Users\Felipe\OneDrive\OneDrive\Área de Trabalho\py-gcp\config\config.yaml", encoding="utf8") as file:
        config = yaml.safe_load(file)
    
    bucket_dw = config["bucket-negocio"]
    credentials_path = config["credentials_path"]
    bucket_processed = config["bucket-processado"]
    project_id = config["project-id"]

    client_bq = bigquery.Client.from_service_account_json(credentials_path)
    modelo = dict_modelo.dict_for_model

    fs = gcsfs.GCSFileSystem()

    # ---- 2. criar lista de arquivos já rodados antes, ou seja, legado
    try:
        df_arquivos_legado = dd.read_csv(f"gs://{bucket_processed}/from-silver-to-gold/*", encoding="iso-8859-1", sep=";")
        df_arquivos_legado = df_arquivos_legado.compute() # converter DataFrame do dask para Pandas
        df_arquivos_legado.rename(columns={df_arquivos_legado.columns[0]: "arquivo"}, inplace=True)

    except:
        df_arquivos_legado = pd.DataFrame({"arquivo": None}, index=[0])

    try:
        list_legado = df_arquivos_legado["arquivo"].to_list()
    except:
        list_legado = []

    list_dfs_executados = []

    for tabela in modelo["dimensoes"]:
        # print(modelo["tabelas"][tabela]["bigquery_name"])

        gcs_path = modelo["dimensoes"][tabela]["gcs_path"]
        extensao = modelo["dimensoes"][tabela]["extension"]
        encoding = modelo["dimensoes"][tabela]["encoding"]
        sep = modelo["dimensoes"][tabela]["sep"]
        surrogate = modelo["dimensoes"][tabela]["surrogate_key"][0]
        natural = modelo["dimensoes"][tabela]["natural_keys"][0]
        fields_for_updates = modelo["dimensoes"][tabela]["fields_for_updates"]

        # método para ler todos os arquivos da pasta - não permite identificar quais serão lidos
        # df = dd.read_csv(f"gcs://{bucket_dw}/silver/{gcs_path}/{tabela}.{extensao}", encoding=encoding, sep=sep)

        # ---- 3. fazer loop pelos arquivos encontrados em silver ---- #
        client_storage = storage.Client.from_service_account_json(credentials_path)
        bucket = client_storage.get_bucket(bucket_dw)

        blob_list = list(bucket.list_blobs(prefix=f"silver/{gcs_path}/{tabela}"))

        list_dfs = []

        # ---- 4. iniciar validação se deve ou não rodar em todos os arquivos do loop ---- #
        for blob in blob_list:
            updated_time = blob.updated
            updated_time -= timedelta(hours=3)
            formatted_time = updated_time.strftime("%Y%m%d%H%M%S")

            id_file = f"{blob.name}_{formatted_time}"

            if not id_file in list_legado: # verificar se deve ou não rodar
                print(f"\nArquivo {blob.name} será processado para gold pois é novo.")
                deve_rodar = True
            else:
                print(f"\nArquivo {blob.name} não será processado para gold pois já foi alimentado no dw antes.")
                deve_rodar = False

            if deve_rodar:
                file_bytes = blob.download_as_bytes()
                file_buffer = io.BytesIO(file_bytes)
                df = pd.read_csv(file_buffer, encoding="iso-8859-1", sep=";")

                list_dfs.append(df)

        try:
            df = pd.concat(list_dfs, ignore_index=True)
        except:
            df = pd.DataFrame()
        
        if not df.empty:
            bigquery_name = modelo["dimensoes"][tabela]["bigquery_name"]
            dataset, table = bigquery_name.split(".")

            table_ref = client_bq.dataset(dataset).table(table)

            # ---- 5. verificar se tabela existe ---- #
            adicionado_a_lista = False
            try:
                checkTable = client_bq.get_table(table_ref, retry=None)
                print(f"A tabela {table} já existe no BigQuery. Verificações serão feitas...")

                fazVerificacoes = True

            except:
                # quando tabela não existir no GBQ, então somente add o contador para todo o DataFrame e depois carregar para o GBQ
                df[surrogate] = df.reset_index().index + 1

                try:
                    df.to_gbq(modelo["dimensoes"][tabela]["bigquery_name"], project_id)
                    print(f"A tabela {table} não existia no BigQuery porém foi criada.")

                    if not adicionado_a_lista:
                        