from pythreader import synchronized, TimerThread, Primitive
import time

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

    def _done(self):
        if self.Connection is not None:
            self.Pool.returnConnection(self.Connection)
            self.Connection = None
            self.Pool = None
    
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
            c.execute("select 1")
            return c.fetchone()[0] == 1
        except:
            return False
        

class ConnectionPool(Primitive):

    def __init__(self, postgres=None, connector=None, idle_timeout = 60):
        Primitive.__init__(self)
        self.IdleTimeout = idle_timeout
        if connector is not None:
            self.Connector = connector
        elif postgres is not None:
            self.Connector = PsycopgConnector(postgres)
        else:
            raise ValueError("Connector must be provided")
        self.IdleConnections = []           # available, [db_connection, ...]
        self.AllConnections = {}            # id(connection) -> (connection, t)
        self.CleanThread = None
        
    @synchronized
    def connect(self):
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
        
    @synchronized
    def returnConnection(self, c):
        if not self.Connector.connectionIsClosed(c) \
                and not c in [x for t, x in self.IdleConnections]:
            self.IdleConnections.append(c)
            self.AllConnections[id(c)] = (c, time.time())
            #print "return: Connections=", self.IdleConnections
        if self.CleanThread is None:
            self.CleanThread = TimerThread(self._cleanUp, self.IdleTimeout/2)
            self.CleanThread.start()
            
    @synchronized
    def _cleanUp(self):
        now = time.time()
        
        new_connections = []
        for c in self.IdleConnections:
            _, t = self.AllConnections[id(c)]
            if t < now - self.IdleTimeout:
                #print "closing idle connection", c
                del self.AllConnections[id(c)]
                c.close()
            else:
                new_connections.append(c)
        self.IdleConnections = new_connections
        
    @synchronized
    def closeAll(self):
        for c, t in self.AllConnections.values():
            c.close()
        self.AllConnections = {}
        self.IdleConnections = []
        
    def __del__(self):
        #print "pool.__del__"
        with self:
            self.closeAll()
            if self.CleanThread is not None:
                self.CleanThread.stop()
                self.CleanThread = None
        
