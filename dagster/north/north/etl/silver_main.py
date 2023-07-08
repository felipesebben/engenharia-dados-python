from north.etl.dw import from_rawdata_to_silver_extract as extract
from north.etl.dw import from_rawdata_to_silver_transform as transform
from north.etl.dw import from_rawdata_to_silver_load as load
import yaml
import datetime

def rodar():
    print("\nIniciando o silver...")

    # ---- 1. alimentar as variáveis a partir do yaml ---- #
    with open(r"C:\Users\Felipe\OneDrive\OneDrive\Área de Trabalho\py-gcp\config\config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    
    bucket_rawdata = config["bucket-rawdata"]; bucket_dw = config["bucket-negocio"]
    bucket_processed = config["bucket-processado"]

    # ---- 2. realizar a extração e a verificação ---- #
    extracao = extract.rodar()
    transformacao = {}

    # ---- 3. verificar se deve ou nao rodar a transformação ---- #
    if not extracao["deve-rodar"]:
        print("Sem necessidade de trasformação.")
    
    else:
        transformacao = transform.rodar(extracao["objetos"])
      
    # ---- 4. verificar se deve ou não rodar a carga ---- #
    if len(transformacao) == 0:
        print("Sem necessidade de loading.")
    
    else:
        carga = load.rodar(transformacao["dict_objetos_finais"])

        if carga["deve-rodar"]:
            # salva os arquivos lidos no processed
            now = datetime.datetime.now()
            timestamp = now.strftime("%Y%m%d %H%M%S")

            df_arquivos_lidos = extracao["df_arquivos_runtime"]
            df_arquivos_lidos.to_csv(f"gs://{bucket_processed}/from-rawdata-to-silver/{timestamp}.csv", encoding="utf-8-sig", sep=";", index=False)
        
        else:
            desc_error = carga["description"]
            print(f"Houve algum erro na tentativa de carga da base final!\n\n{desc_error}")
    

if __name__ == "__main__":
    rodar()
