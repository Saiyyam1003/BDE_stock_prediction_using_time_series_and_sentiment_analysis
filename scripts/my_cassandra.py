from cassandra.cluster import Cluster

cluster = Cluster(['cassandra'])
session = cluster.connect()

# Create keyspace and table
session.execute("""
CREATE KEYSPACE IF NOT EXISTS finance 
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
""")

session.execute("""
CREATE TABLE IF NOT EXISTS finance.stock_data (
    symbol text PRIMARY KEY,
    timestamp text,
    data map<text, float>
);
""")
