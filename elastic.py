from elasticsearch import Elasticsearch

es = Elasticsearch(['your_elasticsearch_host'])

# Create index mapping
mapping = {
  "mappings": {
    "properties": {
      "user_id": {"type": "keyword"},
      "hour_window": {"type": "date"},
      "count": {"type": "integer"}
    }
  }
}

# Create index
es.indices.create(index='socialmediaanalytics', body=mapping)

# Search for data
results = es.search(index='socialmediaanalytics', body={
  "query": {
    "bool": {
      "must": [
        {"match": {"text": "your_query"}}
      ]
    }
  }
})

print(results)
