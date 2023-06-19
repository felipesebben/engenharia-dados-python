from dw import from_silver_to_gold_dimens as dimens
from dw import from_silver_to_gold_facts as facts
from google.cloud import storage
import datetime
import yaml
import pandas as pd


def rodar():
    print("\nIniciando o gold...")

    # ---- 1. alimentar as variáveis a partir do yaml ---- #
    with open(r"C:\Users\Felipe\OneDrive\OneDrive\Área de Trabalho\py-gcp\config\config.yaml", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    
    bucket_processed = config["bucket-processado"]
    project_id = config["project-id"]
    bucket_dw = config["bucket-negocio"]
    credentials_path = config["credentials_path"]

    # ---- 2. criar as dimensões ---- #
    dimensoes = dimens.rodar()

    if dimensoes["deve-rodar"]:
        # salvar os arquivos lidos no processed
        now = datetime.datetime.now()
        timestamp = now.strftime("%Y%m%d %H%M%S")

        df_arquivos_lidos = dimensoes["df_arquivos_runtime"]
        df_arquivos_lidos.to_csv(f"gs://{bucket_processed}/from-silver-to-gold/{timestamp}.csv", encoding="utf-8-sig", sep=";", index=False)
    
    else:
        print("Sem necessidade de rodar as dimensões")
    
    # ---- 3. criar as fatos ---- #
    fatos = facts.rodar()

    if fatos["deve-rodar"]:
        now = datetime.datetime.now()
        timestamp = now.strftime("%Y%m%d %H%M%S")

        # copia um tabelão para um .parquet no bucket gold
        strSQL = """
            select * 
            from `dw_north_sales.fato_pedidos` as a
            left join `dw_north_sales.dim_clientes` as b on a.id_dim_clientes = b.id_dim_clientes
            left join `dw_north_sales.dim_produtos` as c on a.id_dim_produto = c.id_dim_produto
            left join `dw_north_sales.dim_fornecedores`as d on a.id_dim_fornecedores = d.id_dim_fornecedores
            left join `dw_north_sales.dim_funcionarios` as e on a.id_dim_funcionarios = e.id_dim_funcionarios
            left join `dw_north_sales.dim_entregadores` as f on a.id_dim_entregadores = f.id_dim_entregadores
            left join `dw_north_sales.dim_categorias` as g on a.id_dim_categorias = g.id_dim_categorias
        """
        
        #fs = gcsfs.GCSFileSystem(project=project_id, token=credentials_path)
        client_storage = storage.Client.from_service_account_json(credentials_path)

        df_gold = pd.read_gbq(strSQL, project_id=project_id)
        # parquet_data = df_gold.to_parquet("gold_gzip.parquet", index=False, compression="gzip", engine="fastparquet")
        # parquet_data = df_gold.to_parquet("gold_gzip.parquet", index=False, compression="gzip", engine="fastparquet", partition_cols=["id_dim_funcionarios"])

        # ---- 4. acessar o bucket e excluir o parquet antigo ----
        bucket = client_storage.bucket(bucket_dw)
        blobs = bucket.list_blobs(prefix="gold/full_table_gzip.parquet")
        [blob.delete() for blob in blobs]

        df_gold.to_parquet(f"gs://{bucket_dw}/gold/full_table_gzip.parquet", engine="pyarrow", compression="gzip", partition_cols=["id_dim_funcionarios"])
        #df_gold.to_csv(f"gs://{bucket_dw}/gold/{timestamp}.csv", encoding="utf-8-sig", sep=";", index=False)

        # ---- 5. salvar os arquivos lidos no processed ---- #
        df_arquivos_lidos = fatos["df_arquivos_runtime"]
        df_arquivos_lidos.to_csv(f"gs://{bucket_processed}/from-silver-to-gold/{timestamp}.csv", encoding="utf-8-sig", sep=";", index=False)

        return {"df": df_gold.head(),
                "linhas": df_gold.shape[0]}
    
    else:
        print("Sem necessidade de rodar as fatos.")

        return {"df": pd.DataFrame(),
                "linhas": 0}



if __name__ == "__main__":
    rodar()