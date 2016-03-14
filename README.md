# goes

Go ElasticSearch dumper/importer/exporter.

Simple example of usage:

```
# export an index to a file
goes export --in es1.mskdc.lzd.co/test_index --out dump-file.json 

# import an index from a file and assert 3 shards + 0 replicas
goes import --in dump-file.json --out es1.mskdc.lzd.co/test_index2 --shards=3 --repls=0

# copy an index
goes copy --in es1.mskdc.lzd.co/test_index --out es1.mskdc.lzd.co/test_index2 
```

Some of advanced options:
```
# export an index to a file with 10 goroutines and 100 documents per _shard_ per request
goes export --in es1.mskdc.lzd.co/test_index --out dump-file.json --parallel=10 --window=100

# import an index from a file and assert 6 shards + 1 replicas sending 1000 documents per request
goes import --in dump-file.json --out es1.mskdc.lzd.co/test_index2 --bulk=1000 --shards=6 --repls=1
```

In or out parameter can be ommitted for import/export, then the file is read from stdin.
