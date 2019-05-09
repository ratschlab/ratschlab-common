import attr
import pgpasslib
import records
import sqlalchemy
from records import RecordCollection
from sqlalchemy.engine.reflection import Inspector

@attr.s
class PostgresDBParams(object):
    user = attr.ib(default='postgres')
    password = attr.ib(default=None, type=str)
    host = attr.ib(default='localhost')
    db = attr.ib(default='postgres')
    port = attr.ib(default=5432)

    # "disable", "require", "verify-ca" and "verify-full"
    ssl_mode = attr.ib(default='disable')

    def to_jdbc_dict(self):
        if not self.password:
            pw = self._get_passwd_from_pgpass()
        else:
            pw = self.password

        # see also https://jdbc.postgresql.org/documentation/head/connect.html
        return {'user': self.user,
                'password': pw,
                'driver': "org.postgresql.Driver",
                'sslmode': self.ssl_mode
                }

    def database_url(self):
        if self.password:
            name_pwd = '{}:{}'.format(self.user, self.password)
        else:
            name_pwd = self.user  # assume it is in .pgpass

        return "postgres://{}@{}:{}/{}".format(name_pwd, self.host,
                                               self.port, self.db)

    def jdbc_database_url(self):
        return "jdbc:postgresql://{}:{}/{}".format(self.host, self.port,
                                                   self.db)

    def connection(self):
        return sqlalchemy.create_engine(self.database_url())

    def _get_passwd_from_pgpass(self):
        return pgpasslib.getpass(self.host, self.port, self.db, self.user)


# sqlalchemy.inspect(dbp._db._engine)

# TODO better name
class PostgresDBConnectionWrapper:
    def __init__(self, params: PostgresDBParams):
        self._db = records.Database(params.database_url())

    def count_rows(self, table_name, approx=False):
        if not approx:
            q = "SELECT COUNT(*) AS cnt FROM {}".format(table_name)
        else:
            q = "SELECT reltuples::BIGINT AS cnt FROM pg_class WHERE relname " \
                "= '{}'".format(table_name)

        return int(self._db.query(q).next()['cnt'])

    def table_size(self, table_name):
        q = "SELECT pg_relation_size(quote_ident(table_name)) AS size " \
            "FROM information_schema.tables " \
            "WHERE table_name = '{}'".format(table_name)

        r = self._db.query(q).next()
        return int(r['size'])

    def list_tables(self):
        return self._db.get_table_names()

    def raw_query(self, q) -> RecordCollection:
        return self._db.query(q)

    def list_columns(self, table_name):
        return {d['name'] for d in Inspector(self._db._engine).get_columns(
            table_name)}

    def close(self):
        self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
