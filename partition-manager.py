import os
import json
import sys
import asyncio
from datetime import datetime, timedelta, timezone

import asyncpg
from loguru import logger


def setup_logging():
    logger.remove()
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.add(sys.stdout, level=level)


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


async def run():
    setup_logging()

    config_path = os.getenv("CONFIG_PATH")
    if not config_path:
        raise RuntimeError("CONFIG_PATH is not set")

    cfg = load_config(config_path)

    create_ahead_days = int(cfg.get("create_ahead_days", 1))
    retention_days = int(cfg.get("retention_days", 180))
    exchanges = cfg.get("exchanges", {})

    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB")

    db_user = os.getenv("PARTITION_MANAGER_USER")
    db_password = os.getenv("PARTITION_MANAGER_PASSWORD")

    if not all([db_host, db_name, db_user, db_password]):
        raise RuntimeError("Database connection variables are not fully set")

    logger.info("Starting partition-manager")
    logger.info(f"Retention days: {retention_days}, create ahead days: {create_ahead_days}")

    conn = await asyncpg.connect(
        host=db_host,
        port=int(db_port),
        database=db_name,
        user=db_user,
        password=db_password,
    )

    try:
        # фиксируем UTC и на уровне сессии
        await conn.execute("SET TIME ZONE 'UTC'")

        today = datetime.now(timezone.utc).date()
        cutoff_date = today - timedelta(days=retention_days)

        for exchange, params in exchanges.items():
            currencies = params.get("currencies", [])
            tables = params.get("tables", [])

            logger.info(f"Processing exchange: {exchange}")

            for currency in currencies:
                for table in tables:
                    parent = f"{exchange}.{currency}_{table}"
                    logger.info(f"Parent table: {parent}")

                    # создание партиций вперёд
                    for offset in range(0, create_ahead_days + 1):
                        day = today + timedelta(days=offset)
                        await conn.execute(
                            f"SELECT {exchange}.create_daily_partition($1::regclass, $2::date)",
                            parent,
                            day,
                        )
                        logger.info(f"Ensured partition for {parent} on {day}")

                    # очистка старых
                    dropped = await conn.fetchval(
                        f"SELECT {exchange}.drop_partitions_older_than($1::regclass, $2::date)",
                        parent,
                        cutoff_date,
                    )
                    logger.info(f"Dropped {dropped} old partitions for {parent}")

        logger.info("partition-manager finished successfully")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run())
