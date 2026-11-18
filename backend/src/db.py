import os,logging
from sqlalchemy import create_engine,text
from sqlalchemy.orm import sessionmaker, DeclarativeBase
engine = create_engine(os.environ['DATABASE_URL'], pool_pre_ping=True, isolation_level='READ COMMITTED')
Session = sessionmaker(engine, expire_on_commit=False)
class Base(DeclarativeBase): pass
def db_session():
    with Session() as s:
        yield s
# 既有库增补计算恢复列；新库由create_all直接建出。逐列查information_schema，并发重复ALTER可忽略
def ensure_columns():
    cols={'compute_attempt':'INTEGER NOT NULL DEFAULT 0','compute_phase':'VARCHAR(20) NULL','compute_note':'VARCHAR(200) NULL','compute_owner':'VARCHAR(60) NULL','compute_stamp':'VARCHAR(40) NULL'}
    try:
        with engine.begin() as c:
            have={row[0] for row in c.execute(text("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='layouts'"))}
            for name,ddl in cols.items():
                if name not in have:
                    try:c.execute(text(f'ALTER TABLE layouts ADD COLUMN {name} {ddl}'))
                    except Exception:logging.warning('layouts.%s 可能已由并发进程添加',name)
    except Exception:logging.exception('ensure_columns failed')
