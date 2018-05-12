from pythreader import synchronized, TimerThread, Primitive
import time

class ConnectionContext(object):

    def __init__(self, pool, connection):
        self.Connection = connection
        self.Pool = pool
        
    def __enter__(self):
        return self.Connection
        
    def __exit__(self, exc_type, exc_value, traceback):
        self.Pool.returnConnection(self.Connection)
        
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
        self.IdleConnections = []           # available, [(t, db_connection),]
        self.AllConnections = {}        # id(connection) -> connection
        self.CleanThread = None
        
    @synchronized
    def connect(self):
        use_connection = None
        #print "connect(): Connections=", self.IdleConnections
        while self.IdleConnections:
            t, c = self.IdleConnections.pop()
            if self.Connector.probe(c):
                use_connection = c
                break
            else:
                c.close()
        if use_connection is None:
            use_connection = self.Connector.connect()
            self.AllConnections[id(use_connection)] = use_connection
        return ConnectionContext(self, use_connection)
        
    @synchronized
    def returnConnection(self, c):
        if not self.Connector.connectionIsClosed(c):
            self.IdleConnections.append((time.time(), c))
            #print "return: Connections=", self.IdleConnections
        if self.CleanThread is None:
            self.CleanThread = TimerThread(self._cleanUp, self.IdleTimeout/2)
            self.CleanThread.start()
            
    @synchronized
    def _cleanUp(self):
        now = time.time()
        
        new_connections = []
        for t, c in self.IdleConnections:
            if t < now - self.IdleTimeout:
                #print "closing idle connection", c
                del self.AllConnections[id(c)]
                c.close()
            else:
                new_connections.append((t, c))
        self.IdleConnections = new_connections
        
    @synchronized
    def closeAll(self):
        for c in self.AllConnections.values():
            c.close()
        self.AllConnections = {}
        self.IdleConnections = []
        
    def __del__(self):
        print "pool.__del__"
        with self:
            self.closeAll()
            if self.CleanThread is not None:
                self.CleanThread.stop()
                self.CleanThread = None
        
