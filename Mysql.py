import os
import sys
import io
import logging
import time
import boto3
import yaml
import mysql.connector
from mysql.connector.constants import ClientFlag
from pybars import Compiler
from Singleton import singleton
from Util import Timer
from dotenv import load_dotenv, find_dotenv

load_dotenv()

compiler = Compiler()
log = logging.getLogger()
rds = boto3.client('rds')

@singleton
class MysqlDatabase:
    """
    Connect to MySQL database
    """
    def __init__(self, connString=None):
        self.connString = connString
        self.db = None
        self._reconnect()
        self._loadQueryTable()

    def _loadQueryTable(self):
        self._queryTable = {}
        # TODO - perhaps load all .yaml in the `sqlYamlDirectory`
        #for filename in Config.yamlFiles:
        #queryFile = os.path.join(os.environ.get("yamlDirectory"), os.environ.get("yamlFiles"))
        queryFile = './sql/metaApi.yml'
        # TODO - test exists 'fullname'
        with io.open(queryFile, 'r') as f:
            self._queryTable.update(yaml.load(f, Loader=yaml.FullLoader))
            log.debug('%s loaded into SQL query table', queryFile)

    def databaseExists(self, dbname):
        lst = self.listDatabases()
        return bool(dbname in lst)

    def _query(self, *args, **kwargs):
        query = None
        if len(args) < 1:
            raise Exception('No query string specified')

        if self._queryTable.get(args[0], None):
            queryTemplate = compiler.compile(self._queryTable.get(args[0]))
        else:
            queryTemplate = compiler.compile(args[0])
        if len(args) == 2:
            query = queryTemplate(args[1])
        else:
            query = queryTemplate(kwargs)
        return query

    def _reconnect(self):
        hostname = os.environ.get("DB_HOST")
        port = int(os.environ.get("DB_PORT", 3306))
        username = os.environ.get("DB_USERNAME")
        password = os.environ.get("DB_PASSWORD")
        region = "us-east-1"

        config = {
            'user': username,
            'password': password,
            'host': hostname
        }

        if password == 'IAM':
            config.update({
                'password': rds.generate_db_auth_token(hostname, port, username, region),
                'client_flags': [ClientFlag.SSL],
                'ssl_ca': './certs/rds-combined-ca-bundle.pem',
                'auth_plugin': 'mysql_clear_password'
            })

        self.db = None
        backoff = 1
        while self.db is None:
            log.info('Attempting to connect to MySQL',
                     extra={'db_hostname': hostname,  'db_username': username})
            try:
                # self.db = MySQLdb.connect(host=hostname, user=username, passwd=password, ssl=ssl) ;# MySQLdb
                self.db = mysql.connector.connect(**config)
                log.info('MySQL connection successful',
                         extra={'db_hostname': hostname, 'db_username': username})
                cur = self._getCursor()

            except Exception as e:
                print(repr(sys.exc_info()))
                if backoff <= 16:
                    log.warning("Connection attempt failed, retrying in %s", backoff,
                             extra={'db_hostname': hostname, 'db_username': username})
                    time.sleep(backoff)
                    backoff = backoff * 2
                else:
                    log.error("Could not establish MySQL connection",
                              extra={'db_hostname': hostname, 'db_username': username})
                    raise Exception("Could not establish MySQL connection")

    def _getCursor(self):
        cur = None
        try:
            #cur = self.db.cursor(MySQLdb.cursors.DictCursor)  ;# MySQLdb
            cur = self.db.cursor(dictionary=True)  ;# mysql.connector
        except Exception as e:
            self._reconnect()
            cur = self.db.cursor(dictionary=True)  ;# mysql.connector
        if not cur:
            raise Exception("Problem connecting to database")
        return cur

    def _execute(self, *args, **kwargs):
        query = self._query(*args, **kwargs)
        print('------------------------------------------------------------')
        print(query)
        print('------------------------------------------------------------')
        querySignature = args[0]
        cur = self._getCursor()
        with Timer("MysqlDatabase._execute", extra={'query_signature': querySignature, 'query_args': args[1:], 'query_kwargs': kwargs}) as t:
            result = cur.execute(query)
        return (cur, result)

    def INSERT(self, *args, **kwargs):
        cur, rv = self._execute(*args, **kwargs)
        self.db.commit()
        self.db.close()
        return cur.lastrowid

    def DELETE(self, *args, **kwargs):
        cur, rv = self._execute(*args, **kwargs)
        self.db.commit()
        self.db.close()
        return cur.lastrowid

    def UPDATE(self, *args, **kwargs):
        cur, rv = self._execute(*args, **kwargs)
        self.db.commit()
        self.db.close()
        return cur.rowcount

    def SELECT(self, *args, **kwargs):
        cur, rv = self._execute(*args, **kwargs)
        r = cur.fetchall()
        self.db.commit()
        self.db.close()
        return r;

    def BOOLEAN(self, *args, **kwargs):
        cur, rv = self._execute(*args, **kwargs)
        r = cur.fetchall()
        return True if r else False

    def EXECUTE(self, *args, **kwargs):
        cur, rv = self._execute(*args, **kwargs)
        return cur.fetchall()

    def GET_ID(self, *args, **kwargs):
        rv = self.SELECT(*args, **kwargs)
        if len(rv) == 0:
            return None
        return rv[0]['id']

    def GET_IDS(self, *args, **kwargs):
        rv = self.SELECT(*args, **kwargs)
        if len(rv) == 0:
            return None
        return [x['id'] for x in rv]

    def listDatabases(self, basename=None):
        excludes = ('information_schema', 'mysql', 'sys', 'innodb', 'tmp', 'tempDB', 'performance_schema')
        if basename:
            key = 'Database (%s%%)' % basename
            rsp = self.SELECT("show databases like '%s%%'" % basename)
        else:
            key = 'Database'
            rsp = self.SELECT("show databases")

        rv = []
        if rsp:
            rv = [x[key] for x in rsp if x[key] not in excludes]

        return rv

    def listTables(self, dbname, pattern=None):
        rv = []
        if not dbname:
            return []

        key = 'Tables_in_%s' % dbname
        try:
            rsp = self.SELECT('show tables in %s' % dbname)
            if rsp:
                rv = [x[key] for x in rsp]
        except:
            log.exception('Could not find tables like %s', dbname)
            rv = []

        return rv
