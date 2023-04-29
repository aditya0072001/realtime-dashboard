from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth_provider = PlainTextAuthProvider(username='your_username', password='your_password')
cluster = Cluster(['your_cassandra_host'], auth_provider=auth_provider)
session = cluster.connect('your_keyspace')

# Sample code to insert data into Cassandra
query = "INSERT INTO tweets (id, text, created_at, user_id) VALUES (?, ?, ?, ?)"
session.execute(query, (tweet_id, tweet_text, tweet_created_at, user_id))
