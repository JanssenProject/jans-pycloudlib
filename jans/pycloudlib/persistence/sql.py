import contextlib
import logging
import os

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import VARCHAR
from sqlalchemy import TEXT
from sqlalchemy import INT
from sqlalchemy import SMALLINT
from sqlalchemy import TIMESTAMP
from sqlalchemy import JSON
from sqlalchemy import BLOB
from sqlalchemy import BINARY
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import text
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.dialects.mysql import TINYTEXT
from sqlalchemy.dialects.postgresql import BYTEA
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.exc import OperationalError
from sqlalchemy.schema import CreateIndex
from sqlalchemy.schema import Index

from jans.pycloudlib.utils import encode_text

logger = logging.getLogger(__name__)


def get_sql_password() -> str:
    """Get password used for SQL database user.

    :returns: Plaintext password.
    """
    password_file = os.environ.get("CN_SQL_PASSWORD_FILE", "/etc/jans/conf/sql_password")

    password = ""
    with contextlib.suppress(FileNotFoundError):
        with open(password_file) as f:
            password = f.read().strip()
    return password


def get_type_obj(type_, size=None):
    if type_ == "VARCHAR":
        obj = VARCHAR(size)
    elif type_ == "TEXT":
        obj = TEXT
    elif type_ == "SMALLINT":
        obj = SMALLINT
    elif type_ == "INT":
        obj = INT
    elif type_ == "TIMESTAMP":
        obj = TIMESTAMP
    elif type_ == "JSON":
        obj = JSON
    elif type_ == "BLOB":
        obj = BLOB
    elif type_ == "BINARY":
        obj = BINARY
    elif type_ == "BYTEA":
        obj = BYTEA
    elif type_ == "DATETIME":
        obj = DATETIME(fsp=size)
    elif type_ == "TINYTEXT":
        obj = TINYTEXT
    return obj


class SQLClient:
    """This class interacts with SQL database.
    """

    def __init__(self):
        dialect = os.environ.get("CN_SQL_DB_DIALECT", "mysql")
        host = os.environ.get("CN_SQL_DB_HOST", "localhost")
        port = os.environ.get("CN_SQL_DB_PORT", 3306)
        database = os.environ.get("CN_SQL_DB_NAME", "jans")
        user = os.environ.get("CN_SQL_DB_USER", "jans")
        password = get_sql_password()

        if dialect == "mysql":
            connector = "mysql+pymysql"
        else:
            connector = "postgresql+psycopg2"

        self.engine = create_engine(
            f"{connector}://{user}:{password}@{host}:{port}/{database}",
            pool_pre_ping=True,
            hide_parameters=True,
        )
        self.metadata = MetaData(bind=self.engine, reflect=True)

    def is_alive(self):
        """Check whether connection is alive by executing simple query.
        """

        with self.engine.connect() as conn:
            result = conn.execute("SELECT 1 AS is_alive")
            return result.fetchone()[0] > 0

    def create_table(self, table_name, columns_mapping, pk):
        cols = []

        for column_name, data_type in columns_mapping.items():
            types = data_type.split("(")

            if len(types) == 2:
                type_ = types[0]
                size = int(types[1].strip("()"))
            else:
                type_ = types[0]
                size = None

            is_pkey = bool(column_name == pk)
            type_obj = get_type_obj(type_, size)
            cols.append(Column(column_name, type_obj, primary_key=is_pkey))

        table = Table(
            table_name,
            self.metadata,
            *cols,
            extend_existing=True
        )

        try:
            table.create(self.engine)
        except (ProgrammingError, OperationalError) as exc:
            dialect = self.engine.dialect.name

            if dialect == "postgresql" and exc.orig.pgcode in ["42P07"]:
                # error with following code will be suppressed
                # - 42P07: relation exists
                pass
            elif dialect == "mysql" and exc.orig.args[0] in [1050]:
                # error with following code will be suppressed
                # - 1050: table exists
                pass
            else:
                logger.warning(f"Unable to create table {table_name}; reason={exc}")

    def get_table(self, table_name):
        return self.metadata.tables.get(table_name)

    def create_index(self, name, column):
        index = Index(name, column)

        with self.engine.connect() as conn:
            try:
                conn.execute(CreateIndex(index))
            except (ProgrammingError, OperationalError) as exc:
                dialect = self.engine.dialect.name

                if dialect == "postgresql" and exc.orig.pgcode in ["42P07"]:
                    # error with following code will be suppressed
                    # - 42P07: relation exists
                    pass
                elif dialect == "mysql" and exc.orig.args[0] in [1061]:
                    # error with following code will be suppressed
                    # - 1061: duplicate key name (index)
                    pass
                else:
                    logger.warning(f"Unable to create index {name}; reason={exc}")

    def raw_query(self, query, **prepared_data):
        with self.engine.connect() as conn:
            conn.execute(text(query), **prepared_data)


def render_sql_properties(manager, src: str, dest: str) -> None:
    """Render file contains properties to connect to SQL database server.

    :params manager: An instance of :class:`~jans.pycloudlib.manager._Manager`.
    :params src: Absolute path to the template.
    :params dest: Absolute path where generated file is located.
    """

    with open(src) as f:
        txt = f.read()

    with open(dest, "w") as f:
        rendered_txt = txt % {
            "rdbm_db": os.environ.get("CN_SQL_DB_NAME", "jans"),
            "rdbm_type": os.environ.get("CN_SQL_DB_DIALECT", "mysql"),
            "rdbm_host": os.environ.get("CN_SQL_DB_HOST", "localhost"),
            "rdbm_port": os.environ.get("CN_SQL_DB_PORT", 3306),
            "rdbm_user": os.environ.get("CN_SQL_DB_USER", "jans"),
            "rdbm_password_enc": encode_text(
                get_sql_password(),
                manager.secret.get("encoded_salt"),
            ).decode(),
            "server_time_zone": os.environ.get("CN_SQL_DB_TIMEZONE", "UTC"),
        }
        f.write(rendered_txt)
