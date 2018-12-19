# Spelk - reporting Apache Spark metrics to Elasticsearch

## Fork Notes
Note, this is a derivative fork of the original IBM code, which is no longer under development. As such it contains significant changes:

 * uses Elasticsearch Java API - required a bit of shading to get working with Spark
 * makes use dynamic template mappings
 * added support for date-based index names for easier aging off with Curator
 * upgraded to support Elasticsearch 5 and beyond
 * dropped the SparkListener which I currently don't see much benefit in
 
However, to-date I have only tested this on Spark 1.6.3, although plan to support Spark 2.x versions soon.

---

Spelk (spark-elk) is an add-on to Apache Spark (<http://spark.apache.org/>) to report metrics to an Elasticsearch server and allow visualizations using Kibana.

## Building Spelk

Spelk is built using [Apache Maven](http://maven.apache.org/). You can specify the Spark versions.

    mvn clean package -P spark1.6.3

## Configuration

### Elastic index mapping

PUT the dynamic mapping in _mapping.json_ against the following URL:

    http://{{hostname}}:9200/_template/spark-metrics-template
    
This will enable the dynamic generation of any index that starts with _spark-metrics-_. This can be updated to match your preferred naming convention.

### Spark configuration

To use Spelk you need to add the Spelk jar to the Spark driver/executor classpaths and enable the metrics sink. You need to do the following:

 * add the spelk jar file to your _spark.files_ property
 * add the spelk jar file to your _spark.driver.extraClassPath_ property
 * add the spelk jar file to your _spark.executor.extraClassPath_ property

**Enable the Elasticsearch sink**

You will need to create a _metrics.properties_ file, as shown below. Then add this to the following properties:

 * _spark.files_
 * _spark.metrics.conf_ 

Create a metrics.properties file, based on the template below:

    driver.sink.elk.class=org.apache.spark.elk.metrics.sink.ElasticsearchSink
    executor.sink.elk.class=org.apache.spark.elk.metrics.sink.ElasticsearchSink
    #   Name:           Default:      Description:
    #   clusterName     none          Elasticsearch cluster name	
    #   host            none          Elasticsearch server host	
    #   port            none          Elasticsearch server port 
    #   index           spark         Elasticsearch index name
    #   indexDateFormat spark         Elasticsearch index date format, if required - gets appended onto the index name, e,g, yyyy-MM-dd causes the index name to become spark-metrics-2018-12-19
    #   period          10            polling period
    #   units           seconds       polling period units
    *.sink.elk.clusterName=elasticsearch
    *.sink.elk.host=localhost
    *.sink.elk.port=9200
    *.sink.elk.index=spark
    *.sink.elk.period=10
    *.sink.elk.unit=seconds
