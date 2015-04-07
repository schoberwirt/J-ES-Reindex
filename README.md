# J-ES-Reindex

```
Java Command Line tool for reindexing Elasticsearch Indices

$ java -jar J-ES-Reindex.jar 
---------------------------------
Elasticsearch Java Reindex tool v0.5
---------------------------------
Usage: java -jar es-java-cli.jar [OPTIONS]
---------------------------------
Generic Options:
--shost <source_host> [default localhost]
--sport <source_port> [default 9300]
--sclsname <source_cluster_name> [default elasticsearch]
--sidx <source_index_name>
--dhost <destination_host> [default localhost]
--dport <destination_port> [default 9300]
--dclsname <destination_cluster_name> [default elasticsearch]
--didx <destination_index_name>
--usesamecls <use_src_cluster_params(host/port/clsname)_for_dst_cluster> [default false]
---------------------------------
Scroll/Bulk Options:
--scrollsize <scroll_size>  [default 200]
--bulksize <bulk_doc_size>  [default 1000]
--bulkconcreq <bulk_concurrent_requests> [default 5]
--sniff <use_sniffing> [default true]
--bulkqwth <when_bulk_threadpool_q_passes_this_threshold_decrease_bulk_size_by_20%> [default 10]
---------------------------------
Shield options:
--suser <source_cluster_username>
--spass <source_cluster_password>
--duser <destination_cluster_username>
--dpass <destination_cluster_password>


Sample usage(shield basic auth):
java -jar J-ES-Reindes.jar --shost localhost --sport 9300 --sclsname tony_tmp --sidx .marvel-2015.03.31 --suser admin --spass secretpassword --usesamecls true --didx reindexed

Reindexing 16956 documents from localhost:9300//tony_tmp/.marvel-2015.03.31 to localhost:9300//elasticsearch/reindexed
INFO: Indexed 999/16956 [ 05% ] from localhost:9300/.marvel-2015.03.31  to localhost:9300/reindexed 
INFO: Indexed 1999/16956 [ 11% ] from localhost:9300/.marvel-2015.03.31  to localhost:9300/reindexed 
INFO: Indexed 2999/16956 [ 17% ] from localhost:9300/.marvel-2015.03.31  to localhost:9300/reindexed 
INFO: Indexed 3999/16956 [ 23% ] from localhost:9300/.marvel-2015.03.31  to localhost:9300/reindexed 
INFO: Indexed 4999/16956 [ 29% ] from localhost:9300/.marvel-2015.03.31  to localhost:9300/reindexed 
INFO: Indexed 5999/16956 [ 35% ] from localhost:9300/.marvel-2015.03.31  to localhost:9300/reindexed 
INFO: Indexed 6999/16956 [ 41% ] from localhost:9300/.marvel-2015.03.31  to localhost:9300/reindexed 
INFO: Indexed 7999/16956 [ 47% ] from localhost:9300/.marvel-2015.03.31  to localhost:9300/reindexed 
INFO: Indexed 8999/16956 [ 53% ] from localhost:9300/.marvel-2015.03.31  to localhost:9300/reindexed 
INFO: Indexed 9999/16956 [ 58% ] from localhost:9300/.marvel-2015.03.31  to localhost:9300/reindexed 
INFO: Indexed 10999/16956 [ 64% ] from localhost:9300/.marvel-2015.03.31  to localhost:9300/reindexed 
INFO: Indexed 11999/16956 [ 70% ] from localhost:9300/.marvel-2015.03.31  to localhost:9300/reindexed 
INFO: Indexed 12999/16956 [ 76% ] from localhost:9300/.marvel-2015.03.31  to localhost:9300/reindexed 
INFO: Indexed 13999/16956 [ 82% ] from localhost:9300/.marvel-2015.03.31  to localhost:9300/reindexed 
INFO: Indexed 14999/16956 [ 88% ] from localhost:9300/.marvel-2015.03.31  to localhost:9300/reindexed 
INFO: Indexed 15999/16956 [ 94% ] from localhost:9300/.marvel-2015.03.31  to localhost:9300/reindexed 
INFO: Indexed 16955/16956 [ 99% ] from localhost:9300/.marvel-2015.03.31  to localhost:9300/reindexed 

Process finished with exit code 0
```


Library dependencies

commons-codec-1.10.jar
elasticsearch-x.x.x.jar
elasticsearch-shield-x.x.x.jar
json-simple-1.1.1.jar
lucene-code-x.x.x.jar