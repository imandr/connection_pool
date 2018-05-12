class ConnectionContext(object):

    def __init__(self, pool, connection):
        self.Connection = connection
        self.Pool = pool
        
    def __enter__(self):
        return self.Connection
        
    def __exit__(self, exc_type, exc_value, traceback):
        if not self.Connection.closed:
            if exc_type is not None:
                self.Connection.close()
            else:
                self.Pool.addConnection(self.Connection)
        
class ConnectorBase(object):

    def connect(self):
        raise NotImplementedError
        
    def probe(self, connection):
        return True

class PostgresConnector(ConnectorBase):

    def __init__(self, connstr):
        ConnectorBase.__init__(self)
        self.Connstr = connstr
        
    def connect(self):
        return psycopg2.connect(self.Connstr)
        
    def probe(self, conn):
        try:
            c = conn.cursor()
            c.execute("select 1")
            return c.fetchone()[0] == 1
        except:
            return False
        

class ConnectionPool(Primitive):

    def __init__(self, connector, idle_timeout = 60):
        self.IdleTimeout = idle_timeout
        self.Connector = connector
        self.Connections = []       # [(t, db_connection),]
        
    @synchronized
    def connect(self):
        self._cleanUp()
        use_connection = None
        while self.Connections:
            c = self.Connections.pop()
            if self.Connector.probe(c):
                use_connection = c
                break
            else:
                c.close()
        if use_connection is None:
            use_connection = self.Connector.connect()
        return self.ConnectionContext(self, use_connection)
        
    @synchronized
    def addConnection(self, c):
        self.Connections.append((time.time(), c))
        
    def _cleanUp(self):
        now = time.time()
        
        new_connections = []
        for t, c in self.Connections:
            if t < now - self.IdleTimeout:
                c.close()
            else:
                new_connections.append(c)
        self.Connections = new_connections
        
        
            
            
                
        
