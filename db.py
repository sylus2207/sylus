'''
mySQL Query Wrapper in "virident framework"
Till support is built for insert,update,select,delete and count queries
For basic queries pythonic data should be used

TODO:
Robust implementation for raw queries .
It's recommended to use raw_query() for complex queries.
'''
import re, os, sys, time, MySQLdb
from settings import Settings
from logger  import FrameworkException,FrameworkLogger

#ToDo: To handle all the exceptions
LOGGER = FrameworkLogger().logger



class DbSql(object):
	
    def __init__(self, debug=False, host=None,
                 database=None, user=None, password=None, port=None):
        self.debug = debug
        self._load_config(host, database, user, password,port)
        self.con = None
        self._init_db()

    '''
    load configurations from config file
    '''   
    def _load_config(self, host, database, user, password, port):
        # grab the host, database
        settings = Settings()
        if host:
            self.host = host
        else:
            self.host = settings.get_value("DB", "host")
        if database:
            self.database = database
        else:
            self.database = settings.get_value("DB", "database")

        # grab the user and password
        if user:
            self.user = user
        else:
            self.user = settings.get_value("DB", "user")
        if password is not None:
            self.password = password
        else:
            self.password = settings.get_value("DB", "password")
        if port is not None:
            self.port = port
        else :
            self.port = settings.get_value("DB", "port")
        

    def _init_db(self):
        # make sure we clean up any existing connection
        if self.con:
            # disconnect from connection
            self.con.close()
            self.con = None

        # create the db connection and cursor
        self.con = self.connect(self.host, self.database, self.user, self.password)
        self.cur = self.con.cursor()


    def dprint(self, value):
        #debug print
        if self.debug:
            sys.stdout.write('SQL: ' + str(value) + '\n')


    def commit(self):
        # Commit your changes in the database
        self.con.commit()


    def get_last_job_number(self):
        # select last row
        self.cur.execute('SELECT jobid FROM nightlyrun_nightlyjobs ORDER BY jobid DESC LIMIT 1')
        rows = self.cur.fetchall()
        if rows:
            return rows[0][0]
        else:
            return None


    def _quote(self, field):
        # quote your fields
        return '`%s`' % field


    def _where_clause(self, where):
        if not where:
            return '', []
         
        if type( where ) == str:
            #for complex AND OR combinations leave the string as it is
            #else change it to  a dictionary
            if not ( re.search ( '\s+(AND|OR)\s+',where) ):
                #search for multiple params and create a dict incase of multiple params
                if ( re.search ( '\w+=.*,',where )):
                    where_fldlist = where.split(",")
                    where = {}
                    for wherefld  in where_fldlist:
                        fld_list = wherefld.split("=")
                        key = fld_list[0]
                        val = fld_list[1]
                        where[key] = val

        if isinstance(where, dict):
            # key/value pairs (which should be equal, or None for null)
            keys, values = [], []
            for field, value in where.iteritems():
                quoted_field = self._quote(field)
                if value is None:
                    keys.append(quoted_field + ' is null')
                elif '%' in str(value):
                    keys.append(quoted_field + 'like %s')
                    values.append(value)
                else:
                    keys.append(quoted_field + '=%s')
                    values.append(value)
            where_clause = ' and '.join(keys)
        elif isinstance(where, basestring):
            # the exact string
            where_clause = where
            values = []
        elif isinstance(where, tuple):
            # pre formatted where clause + values
            where_clause, values = where
            assert where_clause

        else:
            raise ValueError('Invalid "where" value: %r' % where)

        return ' WHERE ' + where_clause, values


    def count(self,table=None, column_name=None, where=None, distinct=False, group_by=None):
        '''
        returns the number of rows that matches a specified criteria.
        Queries:
        SELECT COUNT(column_name) FROM table_name;
        SELECT COUNT(*) FROM table_name;
        SELECT COUNT(DISTINCT column_name) FROM table_name;
        '''
        cmd = ['select']
        if not column_name:
            column_name = "*"
        if distinct:
            countfield = "COUNT(DISTINCT %s)" %(column_name)
        else:
            countfield = "COUNT(%s)" %(column_name)
        cmd.append(countfield)
        count = self.select(table, '', where, distinct, group_by, cmd)
        return count[0][0]    
            
        
    def select(self, table=None, fields=None, where=None, distinct=False, group_by=None, cmd = None):
        """\
                This selects all the fields requested from a
                specific table with a particular where clause.
                The where clause can either be a dictionary of
                field=value pairs, a string, or a tuple of (string,
                a list of values). 

                For example:
                  where = ("a = %s AND b = %s", ['val', 'val'])
                is better than
                  where = "a = 'val' AND b = 'val'"
        """
        if not table:
			msg = "Table name is mandatory"
			raise DbException(msg)
			
        if not cmd:
            cmd = ['select']
        if distinct:
            cmd.append('distinct')
		
        cmd += [fields, 'from', table]
        where_clause, values = self._where_clause(where)
        cmd.append(where_clause)
		
        if group_by:
            cmd.append(' GROUP BY ' + group_by)

        self.dprint('%s %s' % (' '.join(cmd), values))

        # create a re-runable function for executing the query
        def exec_sql():
            sql = ' '.join(cmd)
            # Execute the SQL command
            numRec = self.cur.execute(sql, values)
            return self.cur.fetchall()

        # run the query, re-trying after operational errors
        return exec_sql()


    def select_sql(self, fields, table, sql, values):
        """
        select fields from table "sql"
        If require will be used in future
        """
        cmd = 'select %s from %s %s' % (fields, table, sql)
        self.dprint(cmd)

        # create a -re-runable function for executing the query
        def exec_sql():
            self.cur.execute(cmd, values)
            return self.cur.fetchall()

        # run the query, re-trying after operational errors
        return exec_sql()


    def _exec_sql_with_commit(self, sql, values):
        try:
            self.cur.execute(sql, values)
            self.con.commit()
        except MySQLdb.OperationalError, e:
            raise FrameworkException(e)
        


    def insert(self, table, data):
        """\
                'insert into table (keys) values (%s ... %s)', values

                data:
                        dictionary of fields and data
        """
        fields = data.keys()
        fields = [f for f in fields if data[f] is not None]
        refs = ['%s' for field in fields if data[field] is not None]
        values = [data[field] for field in fields if data[field] is not None]
        cmd = ('insert into %s (%s) values (%s)' %
               (table, ','.join(self._quote(field) for field in fields),
                ','.join(refs)))
        self.dprint('%s %s' % (cmd, values))

        self._exec_sql_with_commit(cmd, values)


    def delete(self, table, where):
        cmd = ['delete from', table]
        where_clause, values = self._where_clause(where)
        cmd.append(where_clause)
        sql = ' '.join(cmd)
        self.dprint('%s %s' % (sql, values))

        self._exec_sql_with_commit(sql, values)


    def update(self, table, data, where ):
        """\
                'update table set data values (%s ... %s) where ...'

                data:
                        dictionary of fields and data
        """
        cmd = 'update %s ' % table
        fields = data.keys()
        data_refs = [self._quote(field) + '=%s' for field in fields]
        data_values = [data[field] for field in fields]
        cmd += ' set ' + ', '.join(data_refs)

        where_clause, where_values = self._where_clause(where)
        cmd += where_clause

        values = data_values + where_values
        self.dprint('%s %s' % (cmd, values))

        self._exec_sql_with_commit(cmd, values)
        
    def find_job(self, tag):
        table_name = 'nightlyrun_nightlyjobs'
        rows = self.select( table_name,'jobid', tag)
        if rows:
            return rows[0][0]
        else:
            return None

    def find_job_settings(self):
        table_name = 'nightlyrun_nightlyjobs'
        rows = self.select( table_name,'*')
        return rows

    def find_testscript_dets(self):
        table_name = 'nightlyrun_testscripts'
        rows = self.select( table_name,'*')
        return rows

    def connect(self, host, database, user, password):
        #Open database connection
        return MySQLdb.connect(host=host, user=user, passwd=password, db=database)
        
    def update_job(self,data,condition):
        table_name = 'nightlyrun_nightlyjobs'
        table_data= self.format_data(data)
        self.update(table_name,table_data,condition)
        return 0
        
    def insert_job(self,data):
        table_name = 'nightlyrun_nightlyjobs'
        table_data= self.format_data(data)
        self.insert(table_name,table_data)
        return 0
    
    def update_lock(self,data,condition):
        table_name = 'nightlyrun_nightlylockentries'
        table_data= self.format_data(data)
        self.update(table_name,table_data,condition)
        return 0
        
        
    def format_data(self,data):
        if type(data) == str:
            data_list = data.split(",")
            data_dict={}
            for data in data_list:
                data_entity = data.split("=")
                key = data_entity[0]
                key = key.strip()
                val = data_entity[1]
                val = val.strip()
                data_dict[key] = val
            return data_dict
        elif type(data) == dict:
           return data
        return 0