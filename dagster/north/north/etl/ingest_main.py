from north.etl.ingest import from_api_to_rawdata as fapi
from north.etl.ingest import from_file_to_rawdata as ffile
from north.etl.ingest import from_oltp_to_rawdata as foltp

def rodar():
    print("Iniciando o ingest...")

    # fapi.rodar()
    # ffile.rodar()
    foltp.rodar()


if __name__ == "__main__":
    rodar()