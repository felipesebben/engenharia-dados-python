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
        df_arquivos_legado = df_arquivos_legado.compute() # converter objeto desk para DataFrame
        
    except:
        df_arquivos_legado = pd.DataFrame({"arquivo": None}, index=[0])

    list_legado = df_arquivos_legado["arquivo"].to_list()
    print(list_legado)
    
if __name__ == "__main__":
    rodar()