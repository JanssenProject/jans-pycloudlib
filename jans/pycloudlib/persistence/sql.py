import contextlib
import logging
import os

from google.api_core.exceptions import AlreadyExists
from google.api_core.exceptions import FailedPrecondition
from google.cloud import spanner
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import OperationalError

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


class PostgresqlClient:
    def __init__(self):
        host = os.environ.get("CN_SQL_DB_HOST", "localhost")
        port = os.environ.get("CN_SQL_DB_PORT", 5432)
        database = os.environ.get("CN_SQL_DB_NAME", "jans")
        user = os.environ.get("CN_SQL_DB_USER", "jans")
        password = get_sql_password()

        self.engine = create_engine(
            f"{self.connector}://{user}:{password}@{host}:{port}/{database}",
            pool_pre_ping=True,
            hide_parameters=True,
        )
        self.metadata = MetaData(bind=self.engine, reflect=True)

    @property
    def connector(self):
        """Connector name."""

        return "postgresql+psycopg2"

    def connected(self):
        """Check whether connection is alive by executing simple query.
        """

        with self.engine.connect() as conn:
            result = conn.execute("SELECT 1 AS is_alive")
            return result.fetchone()[0] > 0

    def create_table(self, table_name: str, column_mapping: dict, pk_column: str):
        columns = []
        for column_name, column_type in column_mapping.items():
            column_def = f"{self.quoted_id(column_name)} {column_type}"

            if column_name == pk_column:
                column_def += " NOT NULL UNIQUE"
            columns.append(column_def)

        columns_fmt = ", ".join(columns)
        pk_def = f"PRIMARY KEY ({self.quoted_id(pk_column)})"
        query = f"CREATE TABLE {self.quoted_id(table_name)} ({columns_fmt}, {pk_def})"

        with self.engine.connect() as conn:
            try:
                conn.execute(query)
                # refresh metadata as we have newly created table
                self.metadata.reflect()
            except ProgrammingError as exc:
                if exc.orig.pgcode in ["42P07"]:
                    # error with following code will be suppressed
                    # - 42P07: relation exists
                    pass
                else:
                    raise

    def create_index(self, index_name: str, table_name: str, column_name: str):
        query = f"CREATE INDEX {self.quoted_id(index_name)} ON {self.quoted_id(table_name)} ({self.quoted_id(column_name)})"
        self.create_index_raw(query)

    def create_index_raw(self, query):
        with self.engine.connect() as conn:
            try:
                conn.execute(query)
            except ProgrammingError as exc:
                if exc.orig.pgcode in ["42P07"]:
                    # error with following code will be suppressed
                    # - 42P07: relation exists
                    pass
                else:
                    raise

    def quoted_id(self, identifier):
        char = '"'
        return f"{char}{identifier}{char}"

    def get_table_mapping(self) -> dict:
        table_mapping = {}
        for table_name, table in self.metadata.tables.items():
            table_mapping[table_name] = {
                column.name: column.type.__class__.__name__
                for column in table.c
            }
        return table_mapping

    def row_exists(self, table_name, id_):
        table = self.metadata.tables.get(table_name)
        if table is None:
            return False

        query = select([func.count()]).select_from(table).where(
            table.c.doc_id == id_
        )
        with self.engine.connect() as conn:
            result = conn.execute(query)
            return result.fetchone()[0] > 0

    def insert_into(self, table_name, column_mapping):
        table = self.metadata.tables.get(table_name)

        for column in table.c:
            unmapped = column.name not in column_mapping
            is_json = column.type.__class__.__name__.lower() == "json"

            if not all([unmapped, is_json]):
                continue
            column_mapping[column.name] = {"v": []}

        query = table.insert().values(column_mapping)
        with self.engine.connect() as conn:
            try:
                conn.execute(query)
            except IntegrityError as exc:
                if exc.orig.pgcode in ["23505"]:
                    # error with following code will be suppressed
                    # - 23505: unique violation
                    pass
                else:
                    raise


class MysqlClient:
    def __init__(self):
        host = os.environ.get("CN_SQL_DB_HOST", "localhost")
        port = os.environ.get("CN_SQL_DB_PORT", 3306)
        database = os.environ.get("CN_SQL_DB_NAME", "jans")
        user = os.environ.get("CN_SQL_DB_USER", "jans")
        password = get_sql_password()

        self.engine = create_engine(
            f"{self.connector}://{user}:{password}@{host}:{port}/{database}",
            pool_pre_ping=True,
            hide_parameters=True,
        )
        self.metadata = MetaData(bind=self.engine, reflect=True)

    @property
    def connector(self):
        """Connector name."""

        return "mysql+pymysql"

    def connected(self):
        """Check whether connection is alive by executing simple query.
        """

        with self.engine.connect() as conn:
            result = conn.execute("SELECT 1 AS is_alive")
            return result.fetchone()[0] > 0

    def create_table(self, table_name: str, column_mapping: dict, pk_column: str):
        columns = []
        for column_name, column_type in column_mapping.items():
            column_def = f"{self.quoted_id(column_name)} {column_type}"

            if column_name == pk_column:
                column_def += " NOT NULL UNIQUE"
            columns.append(column_def)

        columns_fmt = ", ".join(columns)
        pk_def = f"PRIMARY KEY ({self.quoted_id(pk_column)})"
        query = f"CREATE TABLE {self.quoted_id(table_name)} ({columns_fmt}, {pk_def})"

        with self.engine.connect() as conn:
            try:
                conn.execute(query)
                # refresh metadata as we have newly created table
                self.metadata.reflect()
            except OperationalError as exc:
                if exc.orig.args[0] in [1050]:
                    # error with following code will be suppressed
                    # - 1050: table exists
                    pass
                else:
                    raise

    def create_index(self, index_name: str, table_name: str, column_name: str):
        query = f"CREATE INDEX {self.quoted_id(index_name)} ON {self.quoted_id(table_name)} ({self.quoted_id(column_name)})"
        self.create_index_raw(query)

    def create_index_raw(self, query):
        with self.engine.connect() as conn:
            try:
                conn.execute(query)
            except OperationalError as exc:
                if exc.orig.args[0] in [1061]:
                    # error with following code will be suppressed
                    # - 1061: duplicate key name (index)
                    pass
                else:
                    raise

    def quoted_id(self, identifier):
        char = '`'
        return f"{char}{identifier}{char}"

    def get_table_mapping(self) -> dict:
        table_mapping = {}
        for table_name, table in self.metadata.tables.items():
            table_mapping[table_name] = {
                column.name: column.type.__class__.__name__
                for column in table.c
            }
        return table_mapping

    def row_exists(self, table_name, id_):
        table = self.metadata.tables.get(table_name)
        if table is None:
            return False

        query = select([func.count()]).select_from(table).where(
            table.c.doc_id == id_
        )
        with self.engine.connect() as conn:
            result = conn.execute(query)
            return result.fetchone()[0] > 0

    def insert_into(self, table_name, column_mapping):
        table = self.metadata.tables.get(table_name)

        for column in table.c:
            unmapped = column.name not in column_mapping
            is_json = column.type.__class__.__name__.lower() == "json"

            if not all([unmapped, is_json]):
                continue
            column_mapping[column.name] = {"v": []}

        query = table.insert().values(column_mapping)
        with self.engine.connect() as conn:
            try:
                conn.execute(query)
            except IntegrityError as exc:
                if exc.orig.args[0] in [1062]:
                    # error with following code will be suppressed
                    # - 1062: duplicate entry
                    pass
                else:
                    raise


class SpannerClient:
    def __init__(self):
        # The following envvars are required:
        #
        # - ``GOOGLE_APPLICATION_CREDENTIALS`` json file that should be injected in upstream images
        # - ``GCLOUD_PROJECT`` (a.k.a Google project ID)
        client = spanner.Client()
        instance_id = os.environ.get("GOOGLE_INSTANCE_ID", "")
        self.instance = client.instance(instance_id)

        database_id = os.environ.get("GOOGLE_DATABASE_ID", "")
        self.database = self.instance.database(database_id)

    def connected(self):
        cntr = 0
        with self.database.snapshot() as snapshot:
            result = snapshot.execute_sql("SELECT 1")
            for item in result:
                cntr = item[0]
                break
            return cntr > 0

    def create_table(self, table_name: str, column_mapping: dict, pk_column: str):
        columns = []
        for column_name, column_type in column_mapping.items():
            column_def = f"{self.quoted_id(column_name)} {column_type}"
            columns.append(column_def)

        columns_fmt = ", ".join(columns)
        pk_def = f"PRIMARY KEY ({self.quoted_id(pk_column)})"
        query = f"CREATE TABLE {self.quoted_id(table_name)} ({columns_fmt}) {pk_def}"

        try:
            self.database.update_ddl([query])
        except FailedPrecondition as exc:
            if "Duplicate name in schema" in exc.args[0]:
                # table exists
                pass
            else:
                raise

    def quoted_id(self, identifier):
        char = '`'
        return f"{char}{identifier}{char}"

    def get_table_mapping(self) -> dict:
        def parse_field_type(type_):
            name = type_.code.name
            if name == "ARRAY":
                name = f"{name}<{type_.array_element_type.code.name}>"
            return name

        table_mapping = {}
        for table in self.database.list_tables():
            table_mapping[table.table_id] = {
                field.name: parse_field_type(field.type_)
                for field in table.schema
            }
        return table_mapping

    def insert_into(self, table_name, column_mapping):
        # TODO: handle ARRAY<STRING(MAX)> ?
        def insert_rows(transaction):
            transaction.insert(
                table_name,
                columns=column_mapping.keys(),
                values=[column_mapping.values()]
            )

        try:
            self.database.run_in_transaction(insert_rows)
        except AlreadyExists:
            pass

    def row_exists(self, table_name, id_):
        exists = False
        with self.database.snapshot() as snapshot:
            result = snapshot.read(
                table=table_name,
                columns=["doc_id"],
                keyset=spanner.KeySet([
                    [id_]
                ])
            )
            for _ in result:
                exists = True
                break
        return exists

    def create_index(self, index_name: str, table_name: str, column_name: str):
        raise NotImplementedError

    def create_index_raw(self, query):
        raise NotImplementedError


class SQLClient:
    """This class interacts with SQL database.
    """

    def __init__(self):
        dialect = os.environ.get("CN_SQL_DB_DIALECT", "mysql")
        if dialect in ("pgsql", "postgresql"):
            self.adapter = PostgresqlClient()
        elif dialect == "mysql":
            self.adapter = MysqlClient()
        elif dialect == "spanner":
            self.adapter = SpannerClient()

    def is_alive(self):
        # DEPRECATED
        return self.connected()

    def connected(self):
        return self.adapter.connected()

    def create_table(self, table_name, columns_mapping, pk):
        return self.adapter.create_table(table_name, columns_mapping, pk)

    def get_table_mapping(self):
        return self.adapter.get_table_mapping()

    def create_index_raw(self, query):
        return self.adapter.create_index_raw(query)

    def create_index(self, index_name, table_name, column_name):
        return self.adapter.create_index(index_name, table_name, column_name)

    def quoted_id(self, identifier):
        return self.adapter.quoted_id(identifier)

    def row_exists(self, table_name, id_):
        return self.adapter.row_exists(table_name, id_)

    def insert_into(self, table_name, column_mapping):
        return self.adapter.insert_into(table_name, column_mapping)


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
