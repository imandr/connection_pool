import time
from threading import RLock, Thread

class _WrappedConnection(object):
    #
    # The pool can be used in 2 ways:
    #
    # 1. explicit connection
    #   
    #   conn = pool.connect()
    #   ....
    #   conn.close()
    #
    #   conn = pool.connect()
    #   ...
    #   # connection goes out of scope and closed during garbage collection
    #
    # 2. via context manager
    #
    #   with pool.connect() as conn:
    #       ...
    #

    def __init__(self, pool, connection):
        self.Connection = connection
        self.Pool = pool
        
    def __str__(self):
        return "WrappedConnection(%s)" % (self.Connection,)
        
    __repr__ = __str__

    def _done(self):
        if self.Pool is not None:
            self.Pool.returnConnection(self.Connection)
            self.Pool = None
        if self.Connection is not None:
            self.Connection = None
    
    #
    # If used via the context manager, unwrap the connection
    #
    def __enter__(self):
        return self.Connection
        
    def __exit__(self, exc_type, exc_value, traceback):
        self._done()
    
    #
    # If used as is, instead of deleting the connection, give it back to the pool
    #
    def __del__(self):
        self._done()
        
    #
    # If closed explicitly, close it and do not return to the pool
    #
    def close(self):
        if self.Connection is not None:
            self.Connection.close()
            self.Connection = None
        self.Pool = None
    
    #
    # act as a database connection object
    #
    def __getattr__(self, name):
        return getattr(self.Connection, name)

class ConnectorBase(object):

    def connect(self):
        raise NotImplementedError
        
    def probe(self, connection):
        return True
        
    def connectionIsClosed(self, c):
        raise NotImplementedError
        

class PsycopgConnector(ConnectorBase):

    def __init__(self, connstr):
        ConnectorBase.__init__(self)
        self.Connstr = connstr
        
    def connect(self):
        import psycopg2
        return psycopg2.connect(self.Connstr)
        
    def connectionIsClosed(self, conn):
        return conn.closed
        
    def probe(self, conn):
        try:
            c = conn.cursor()
            c.execute("rollback; select 1")
            alive = c.fetchone()[0] == 1
            c.execute("rollback")
            return alive
        except:
            return False
            
class MySQLConnector(ConnectorBase):
    def __init__(self, connstr):
        raise NotImplementedError

class _ConnectionPool(object):      
    #
    # actual pool implementation, without the reference to the CleanUpThread to avoid circular reference
    # between the pool and the clean-up thread
    #

    def __init__(self, postgres=None, mysql=None, connector=None):
        if connector is not None:
            self.Connector = connector
        elif postgres is not None:
            self.Connector = PsycopgConnector(postgres)
        elif mysql is not None:
            self.Connector = MySQLConnector(mysql)
        else:
            raise ValueError("Connector must be provided")
        self.IdleConnections = []           # available, [db_connection, ...]
        self.AllConnections = {}            # id(connection) -> (connection, t)
        self.Lock = RLock()
        
    def connect(self):
        with self.Lock:
            use_connection = None
            #print "connect(): Connections=", self.IdleConnections
            while self.IdleConnections:
                c = self.IdleConnections.pop()
                if self.Connector.probe(c):
                    use_connection = c
                    break
                else:
                    c.close()
                    del self.AllConnections[id(c)]
            else:
                use_connection = self.Connector.connect()
                self.AllConnections[id(use_connection)] = (use_connection, time.time())
            return _WrappedConnection(self, use_connection)
        
    def returnConnection(self, c):
        with self.Lock:
            if self.Connector.connectionIsClosed(c):
                try:    del self.AllConnections[id(c)]
                except KeyError:    pass
                return
            
            if not c in self.IdleConnections:
                self.IdleConnections.append(c)
                self.AllConnections[id(c)] = (c, time.time())
            
    def _cleanUp(self, idle_timeout):
        with self.Lock:
            now = time.time()
            new_connections = []
            for c in self.IdleConnections:
                _, t = self.AllConnections[id(c)]
                if t < now - idle_timeout:
                    #print "closing idle connection", c
                    del self.AllConnections[id(c)]
                    c.close()
                else:
                    new_connections.append(c)
            self.IdleConnections = new_connections
        
    def closeAll(self):
        with self.Lock:
            for c, t in self.AllConnections.values():
                c.close()
            self.AllConnections = {}
            self.IdleConnections = []
            
class CleanUpThread(Thread):    
    
    def __init__(self, pool, timeout):
        Thread.__init__(self)
        self.Timeout = timeout
        self.Pool = pool
        self.Stop = False
        
    def run(self):
        while not self.Stop:
              time.sleep(float(self.Timeout)/2)
              self.Pool._cleanUp(self.Timeout)
    
    def stop(self):
        self.Stop = True
    
              
class ConnectionPool(object):

    #
    # This class is needed only to break circular dependency between the CleanUpThread and the real Pool
    # The CleanUpThread will be owned by this Pool object, while pointing to the real Pool
    #
    
    def __init__(self, idle_timeout = 60, *params, **args):
        self.Pool = _ConnectionPool(*params, **args)    # the real pool
        self.CleanThread = CleanUpThread(self.Pool, idle_timeout)
        self.CleanThread.start()

    def __del__(self):
        # make sure to stop the clean up thread
        self.CleanThread.stop()
        self.CleanThread = None

    # delegate all functions to the real pool
    def __getattr__(self, name):
        return getattr(self.Pool, name)
        
