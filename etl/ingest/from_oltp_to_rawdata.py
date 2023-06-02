import psycopg2 # Conectar com postgres
import pandas as pd
import yaml
from google.cloud import storage # Conectar com storage GCP

if __name__ == '__main__':
    with open(r"C:\Users\Felipe\OneDrive\OneDrive\Área de Trabalho\py-gcp\config\config.yaml", encoding="utf8") as file: #'r' para interpetar string como bruta
        config = yaml.safe_load(file) #definir arquivo como yaml

    db_name = config['connector-oltp']['db_name']
    db_user = config['connector-oltp']['db_user']
    db_password = config['connector-oltp']['db_password']
    db_host = config['connector-oltp']['db_host']
    bucket_raw = config['bucket-rawdata']
    credentials_path = config['credentials_path']
    db_iduser = config['connector-oltp']['db_iduser']
    
    # ---- conecte ao banco de dados ---- 
    conn = psycopg2.connect(database=db_name, user=db_user, password=db_password, host=db_host)

    # ---- obtem a lista de tabela ----
    query = f"""
    SELECT n.nspname AS schema,
        t.relname AS table_name,
        t.relkind AS type,
        t.relowner::regrole AS owner,
        t.relowner 
    FROM pg_class AS t
    JOIN pg_namespace AS n ON t.relnamespace = n.oid
    /* only tables and partitioned tables */
    WHERE t.relkind IN ('r', 'p') and t.relowner = {db_iduser}
    """
    
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()

    tabelas = [result[1] for result in results]

    tabelas_permitidas = ['customers', 'employees', 'categories', 'products', 'suppliers', 'orders', 'shippers', 'region', 'territories', 'order_details']

    tabelas_intereseccao = list(set(tabelas) & set(tabelas_permitidas)) # selecionar apenas tabelas conjuntas no set e armazenar em lista

    # setar a variavel do storage
    client_storage = storage.Client.from_service_account_json(credentials_path)

    for tabela in tabelas_intereseccao:
        df = pd.read_sql_query(f"SELECT * FROM {tabela}", conn)
        
        if df.shape[0] > 0: # verificar se DataFrame nao esta vazio
            folder_path = f"oltp-north/{tabela}/{tabela}.csv"
            bucket = client_storage.bucket(bucket_raw)

            # cria csv a aprtir do DataFrame
            csv_data = df.to_csv(index=False, encoding="utf-8-sig", sep=";")
            csv_data = csv_data.encode("iso-8859-1") # encode considerando caracteres latinos

            # upload para o storage
            blob = bucket.blob(folder_path) # criar id para o path indicado
            blob.upload_from_string(csv_data)

            # exemplo gs://
            if tabela == "order_details":
                df.to_csv(f"gs://{bucket_raw}/{folder_path}", endcoding="utf-8-sig", index=False)
            
            print(f"Tabela {tabela} carregada no destino")
        
        else:
            print(f"Tabela {tabela} vazia, portanto, não carregada ao storage")
