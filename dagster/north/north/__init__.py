from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)
from . import assets


all_assets = load_assets_from_modules([assets])
# Define a job that will materialize the assets
north_job = define_asset_job("north_job", selection=AssetSelection.all())
# Addition: a ScheduleDefinition the job it should run and a cron schedule of how frequently to run it
job_schedule = ScheduleDefinition(
    job=north_job,
    #cron_schedule="5 * * * *",  #no minuto 5
    cron_schedule="*/10 * * * *",  #cada 10 minutos
    #cron_schedule="0 * * * *",  #cada 1 hora
    #cron_schedule="*/5 * * * *",  #cada 5 minutos
    #cron_schedule="5 4 * * *" //sempre as 4:05
    #cron_schedule="5 0 * 8 *" //todo o mes de agosto sempre as 00:05
    #cron_schedule="5 4 * * sun" //sempre as 04:05 de cada sábado
    #cron_schedule="30 * 8-19 * *" //No minuto 30, todos os dias do mês, das 8 às 19.
    #cron_schedule="30 8 * * mon-fri" //Às 08h30 todos os dias da semana, de segunda a sexta-feira.
)
defs = Definitions(
    assets=all_assets,
    schedules=[job_schedule],
)