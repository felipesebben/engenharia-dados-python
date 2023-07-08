from dagster import asset, get_dagster_logger, Output, MetadataValue # import the `dagster` library
import pandas as pd # Add new imports to the top of `assets.py`
from north.etl import ingest_main as ing
from north.etl import silver_main as sil
from north.etl import gold_main as gol
import logging
import sys
from datetime import datetime


def configurar_log_console(nome_arquivo):
    """
    Configura local para salvar log em .txt
    """
    logging.basicConfig(level=logging.INFO, filename=nome_arquivo, filemode='w')
    redirecionar_stdout()


def redirecionar_stdout():
    """
    Redireciona os prints para um arquivo .txt
    """
    class RedirecionarStdout:
        def _init_(self, logger, orig_stdout):
            self.logger = logger
            self.orig_stdout = orig_stdout
        def write(self, mensagem):
            self.logger.info(mensagem)
            self.orig_stdout.write(mensagem)
        def flush(self):
            self.orig_stdout.flush()
    logger = logging.getLogger()
    orig_stdout = sys.stdout
    sys.stdout = RedirecionarStdout(logger, orig_stdout)


def set_time():
    """
    Criar arquivo com data e hora atual na execução.
    """
    data_hora_atual = datetime.now()
    data_hora_formatada = data_hora_atual.strftime('%Y%m%d %H%M%S')
    return data_hora_formatada


@asset # add the asset decorator to tell Dagster this is an asset
def ingest(): # turn it into a function
    """Copia do OLTP"""
    tempo = set_time()
    # configurar_log_console(f'C:/Users/Felipe/OneDrive/OneDrive/Área de Trabalho/py-gcp/dagster/north/logs/{tempo}_ingest.txt')
    ing.rodar()
    return tempo

@asset
def silver(ingest):
    """Execução da camada Silver"""
    # configurar_log_console(f'C:/Users/Felipe/OneDrive/OneDrive/Área de Trabalho/py-gcp/dagster/north/logs/{ingest}_silver.txt')
    sil.rodar()
    return ingest


@asset
def gold(silver):
    """Carga para o BigQuery"""
    # configurar_log_console(f'C:/Users/Felipe/OneDrive/OneDrive/Área de Trabalho/py-gcp/dagster/north/logs/{silver}_gold.txt')
    resposta = gol.rodar()
    return Output(  # The return value is updated to wrap it in `Output` class
        value=resposta['df'],   # The original df is passed in with the `value` parameter
        metadata={
            "LinhasNaFato": resposta['linhas'], # Metadata can be any key-value pair
            "Preview": MetadataValue.md(resposta['df'].to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
)
