import attr
import pgpasslib
import records
from records import RecordCollection
import sqlalchemy

@attr.s
class PostgresDBParams(object):
    user = attr.ib(default='postgres')
    password = attr.ib(default=None, type=str)
    host = attr.ib(default='localhost')
    db = attr.ib(default='postgres')
    port = attr.ib(default=5432)
    current_schema = attr.ib('public')

    # "disable", "require", "verify-ca" and "verify-full"
    ssl_mode = attr.ib(default='disable')

    def to_jdbc_dict(self):
        if not self.password:
            pw = self._get_passwd_from_pgpass()
        else:
            pw = self.password

        use_ssl = False
        if self.ssl_mode != 'disable':
            use_ssl = True

        # see also https://jdbc.postgresql.org/documentation/head/connect.html
        return {'user': self.user,
                'password': pw,
                'driver': "org.postgresql.Driver",
                'currentSchema': self.current_schema,
<<<<<<< HEAD
                'sslMode': self.ssl_mode
||||||| merged common ancestors
                'ssl': str(self.use_ssl)
=======
                'sslMode': self.ssl_mode,
                'ssl': use_ssl
>>>>>>> ae05f4c3f68cee5090ef94996fe6fdda003fc278
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

    def close(self):
        self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
