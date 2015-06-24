# es-downloader

A simple tool written in Java to download documents from Elastic Search.

# Building

Build standalone jar

```
mvn clean assembly:assembly
```

To build an executable, combine the standalone jar built from above with the stub bash script like so:

```
cd bin
rm es-downloader
cat stub.sh ../target/es-downloader-0.0.1-SNAPSHOT-jar-with-dependencies.jar > es-downloader
chmod +x es-downloader
```
The resulting `es-downloader` file can be executed from the command line

# Running

```
> cd bin
> ./es-downloader
usage: es-downloader -[OPTION] [value]
 -c <arg>   (Optional) Elasticsearch cluster name. Default: elasticsearch
 -h <arg>   (Required) Elasticsearch host name.
 -i <arg>   (Optional) Regex of index names to download. Default: .*
 -o <arg>   (Optional) Output directory path. Default: /tmp/elastic
 -p <arg>   (Optional) Elasticsearch port. Default: 9300

> ./es-downloader -h elasticsearch.somewhere.com -p 9300 -c elasticsearch -i myindex.*
```

Enjoy. :shipit: