from __future__ import annotations

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from mysql.connector import pooling

from .jobs import log_execution_time, refresh_mv_job, routeviolation_completo, tags_job
from .settings import SchedulerSettings


def configure_scheduler(*, settings: SchedulerSettings, pool: pooling.MySQLConnectionPool) -> None:
    # `pool` fica aqui para futuramente jobs que usem DB diretamente; por enquanto não usa.
    _ = pool

    from grid import processar_grid
    from odometer import main as odometer_main
    from remover_rotas_canceladas import remover_rotas_canceladas
    from ultima_execucao import atualizar_ultima_execucao

    scheduler = BackgroundScheduler()

    scheduler.add_job(
        func=log_execution_time(processar_grid),
        trigger="interval",
        minutes=60,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        func=log_execution_time(atualizar_ultima_execucao),
        trigger="interval",
        minutes=60,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        func=log_execution_time(routeviolation_completo),
        trigger="interval",
        minutes=60,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        func=refresh_mv_job,
        trigger="interval",
        minutes=60,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        func=log_execution_time(tags_job),
        trigger="cron",
        minute=0,
        hour="*/2",
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        func=log_execution_time(remover_rotas_canceladas),
        trigger="cron",
        minute=0,
        hour="*/2",
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        func=log_execution_time(odometer_main),
        trigger="interval",
        minutes=60,
        max_instances=1,
        coalesce=True,
    )

    try:
        scheduler.start()
        logging.info("Agendador iniciado com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao iniciar o agendador: {e}")
