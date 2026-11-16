"""MARTS: build the tables Metabase reads, from the warehouse tables.

    python etl/marts.py [--db-url URL]

Every mart is a plain table in the marts schema, rebuilt from scratch on each
run. That is cheap (the warehouse is about half a million rows), it cannot drift
from the warehouse, and a re-run always gives the same result. The whole build is
one transaction: if any mart fails or a check fails, the previous marts stay.

Connect to metabase later
"""
import argparse
import logging
import os
import re
import sys
import time

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from . import config
from .load import make_engine


def build_marts(engine=None) -> dict:
    engine = engine or make_engine()
    m, w = config.DB_MARTS_SCHEMA, config.DB_SCHEMA
    start = time.time()
    rows = {}
    try:
        with engine.begin() as conn:
            conn.execute(text("SELECT pg_advisory_xact_lock(hashtext('kfd_warehouse_load'))"))
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {m}"))
            
            # 1. Clean up existing tables
            for name in reversed(config.MART_TABLES):
                conn.execute(text(f"DROP TABLE IF EXISTS {m}.{name} CASCADE"))
            
            # 2. Execute the entire SQL script ONCE
            sql = config.SCHEMA_MARTS_SQL_PATH.read_text(encoding="utf-8")
            sql = sql.format(
                w="warehouse",
                m="marts",
                shock_values="(10), (25), (50)"
                )
            
            conn.execute(text(sql))
            
            # 3. Loop only to count rows for each table
            for name in config.MART_TABLES:
                t0 = time.time()
                rows[name] = conn.execute(text(f"SELECT count(*) FROM {m}.{name}")).scalar_one()
                # log.info("Built %s: %d rows in %.1fs", name, rows[name], time.time() - t0)
                
    except SQLAlchemyError as exc:
        raise(exc)
        
    return rows

