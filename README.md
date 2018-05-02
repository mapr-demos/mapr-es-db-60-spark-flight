#  Fast data processing pipeline for predicting flight delays using Apache APIs: Kafka, Spark, Drill, and MapR-DB

## Introduction

This example will show you how to work with MapR-ES, Spark Streaming, and MapR-DB JSON :

* Build and save a machine learning model using Spark ML 
* Publish using the Kafka API flight data from a JSON file into MapR-ES 
* Consume, enrich, and publish streaming JSON data with Spark ML, Spark Streaming, and the Kafka API.
* Consume enriched JSON data and save to the MapR-DB document database using the Spark-DB connector.
* Query and Load the JSON data from the MapR-DB document database using the Spark-DB connector and Spark SQL.
* Query the MapR-DB document database using Apache Drill. 
* Query the MapR-DB document database using Java and the OJAI library.

**Prerequisites**

* MapR Converged Data Platform 6.0 with Apache Spark and Apache Drill OR [MapR Container for Developers](https://maprdocs.mapr.com/home/MapRContainerDevelopers/MapRContainerDevelopersOverview.html).
* JDK 8
* Maven 3.x (and or IDE such as Netbeans or IntelliJ )

## Setting up MapR Container For Developers

The MapR Container For Developers is a docker image that enables you to quickly deploy a MapR environment on your developer machine.

Install the MapR Container For Developers following the setup information [**here**](https://maprdocs.mapr.com/home/MapRContainerDevelopers/MapRContainerDevelopersOverview.html).

#### 1. Create MapR-ES Stream, Topic, and MapR-DB table

After the container has started, from your mac terminal log in to the docker container:
```
$ ssh root@maprdemo -p 2222
```
In the docker container use the mapr command line interface to create a stream, 2 topics, get info and create a table:
```
maprcli stream create -path /apps/stream -produceperm p -consumeperm p -topicperm p
maprcli stream topic create -path /apps/stream -topic flights -partitions 3
maprcli stream topic create -path /apps/stream -topic flightp -partitions 3  

maprcli stream topic info -path /apps/stream -topic flights

maprcli table create -path /mapr/maprdemo.mapr.io/apps/flights -tabletype json -defaultreadperm p -defaultwriteperm p
  
```

> Refer to [**stream create command**](https://maprdocs.mapr.com/home/ReferenceGuide/stream_create.html) and
 [**table create command**](https://maprdocs.mapr.com/home/ReferenceGuide/table-create.html) 
 for more details about the CLI command.


#### 2. Copy the flight data files onto to MapR-XD cluster filesystem

From a mac terminal window , in your project directory

If MapR NFS is installed and the cluster is mounted at /mapr then you can use cp to copy files. 

The MapR Container For Developers does not include MapR NFS, so you will need to use the hadoop command to put the JSON files on the MapR filesystem.
Run hadoop fs commands to put the data:

```
hadoop fs -put ./data/*.json /tmp/
```

this will put the data files into the cluster directory: 
/mapr/<cluster-name>/tmp/

#### 3.  Build project with maven and/or load into your IDE

**Build the project  with maven (not necessary if you are running from your IDE)**

from your mac terminal use maven to build the project

```
$ mvn clean install
```

This creates the following jars in the target directory.

- `mapr-es-db-60-spark-flight-1.0.jar`
- `mapr-es-db-60-spark-flight-1.0-jar-with-dependencies.jar`

**Or Import your project into Intellij IDE**

IntelliJ
Install IntelliJ from https://www.jetbrains.com/idea/download/
Add the scala language plugin
Import the code as a maven project and let it build


#### 4. Run the Spark program which will create and save the machine learning model


From your mac in the mapr-es-db-spark-payment directory you can run the Spark program which will create and save the machine learning model 
or you can run from your IDE by right mouse clicking on the ml.Flight file and selecting run:

```
$ java -cp ./target/mapr-es-db-60-spark-flight-1.0.jar:./target/* ml.Flight
```
This client will read  from the files hdfs:///mapr/maprdemo.mapr.io/tmp/flights20170102.json, hdfs:///mapr/maprdemo.mapr.io/tmp/flights20170304.json
and save the model in hdfs:///mapr/maprdemo.mapr.io/tmp/flightmodel  
You can optionally pass the files  as input parameters <file file modelpath>   (take a look at the code to see what it does)


#### 5. Run the Java client to publish events to the topic 


From your mac in the mapr-es-db-spark-payment directory you can run the java client to publish with the following command 
or you can run from your IDE by right mouse clicking on the streams.MsgProducer java file and selecting run:

```
$ java -cp ./target/mapr-es-db-60-spark-flight-1.0.jar:./target/* streams.MsgProducer
```
This client will read lines from the file in ./data/flights20170304.json and publish them to the topic /apps/stream:flights. 
You can optionally pass the file and topic as input parameters <file topic> 

#### 6. Run the  the Spark Streaming client to consume events enrich them and publish to another topic

You can wait for the java client to finish, or from a separate mac terminal you can run the spark streaming consumer with the following command, 
or you can run from your IDE by right mouse clicking on the stream.SparkKafkaConsumerProducer scala file and selecting run:

```
$ java -cp ./target/mapr-es-db-60-spark-flight-1.0.jar-with-dependencies.jar:./target/* stream.SparkKafkaConsumerProducer
```
This spark streaming client will consume from the topic /apps/stream:flights and publish to the /apps/stream:flightp topic.
You can optionally pass the topics as input parameters <topic topic> 

#### 7. Run the  the Spark Streaming client to consume enriched events and write to MapR-DB with Spark-DB Connector

You can wait for the 2 publishers to finish, or from a separate mac terminal you can run the spark streaming consumer with the following command, 
or you can run from your IDE :

```
$ java -cp ./target/mapr-es-db-60-spark-flight-1.0.jar-with-dependencies.jar:./target/* stream.SparkKafkaConsumeWriteMapRDB
```
This spark streaming client will consume from the topic /apps/stream:flightp  and write to the table /apps/flights.
You can optionally pass the topic and table as input parameters <topic table> 


#### 8. Run the Spark SQL client to load and query data from MapR-DB with Spark-DB Connector

You can wait for the java client and spark consumers to finish, or from a separate mac terminal you can run the spark sql with the following command, 
or you can run from your IDE :

```
$ java -cp ./target/mapr-es-db-60-spark-flight-1.0.jar:./target/* sparkmaprdb.QueryFlight
```

optionally from your mac you can log in to the docker container:

```
$ ssh root@maprdemo -p 2222
```

start the spark shell with this command

```
$ /opt/mapr/spark/spark-2.1.0/bin/spark-shell --master local[2]
```
copy paste  from the scripts/sparkshell file to query MapR-DB

#### 9. Working with Drill-JDBC

From your mac in the mapr-es-db-spark-payment directory you can run the java client to query the MapR-DB table using Drill-JDBC 
or you can run from your IDE :

```
$ java -cp ./target/mapr-es-db-60-spark-flight-1.0.jar:./target/* maprdb.DRILL_SimpleQuery
```

#### 10. Working with OJAI

OJAI the Java API used to access MapR-DB JSON, leverages the same query engine as MapR-DB Shell and Apache Drill. 

From your mac in the mapr-es-db-60-spark-flight  directory you can run the java client to query the MapR-DB table using OJAI
or you can run from your IDE :

```
$ java -cp ./target/mapr-es-db-60-spark-flight-1.0.jar:./target/* maprdb.OJAI_SimpleQuery
```

#### 11. Using the MapR-DB shell and Drill from your mac client 


**Use MapR DB Shell to Query the Payments table**

In this section you will  use the DB shell to query the Flights JSON table

To access MapR-DB from your mac client or logged into the container, you can use MapR-DB shell:

```
$ /opt/mapr/bin/mapr dbshell
```

To learn more about the various commands, run help or help <command> , for example help insert.

```
$ maprdb mapr:> jsonoptions --pretty true --withtags false
```
**find 5 documents**
```
maprdb mapr:> find /apps/flights --limit 5
```
```
maprdb mapr:> find /apps/flights --where '{ "$eq" : {"origin":"ATL"} }' --f _id,origin,dest,pred_dtree
```
Note that queries by _id will be faster because _id is the primary index

**find all of the Atlanta flights predicted late**
```
maprdb mapr:> find /apps/flights --where '{"$and":[{"$eq":{"pred_dtree":1.0}},{ "$like" : {"_id":"%ATL%"} }]}' --f _id,pred_dtree
```
**find all of the SFO->DEN flights that were late**
```
find /apps/flights --where '{"$and":[{"$eq":{"pred_dtree":1.0}},{ "$like" : {"_id":"%SFO_DEN%"} }]}' --f _id,pred_dtree
```
**find all of the american airlines flights predicted late**
```
maprdb mapr:> find /apps/flights --where '{"$and":[{"$eq":{"pred_dtree":1.0}},{ "$like" : {"_id":"AA%"} }]}' --f _id,pred_dtree
```

**Use Drill Shell to query MapR-DB**
Refer to [**connecting clients **](https://maprdocs.mapr.com/home/MapRContainerDevelopers/ConnectingClients.html) for 
information on setting up the Drill client

From your mac terminal connect to Drill as user mapr through JDBC by running sqlline:
/opt/mapr/drill/drill-1.11.0/bin/sqlline -u "jdbc:drill:drillbit=localhost" -n mapr


**what is the count of predicted delay/notdelay by scheduled departure hour**
```
0: jdbc:drill:drillbit=localhost> select crsdephour, pred_dtree, count(pred_dtree) as countp from dfs.`/apps/flights` group by crsdephour, pred_dtree order by crsdephour;
```
**what is the count of predicted delay/notdelay by origin**
```
> select origin, pred_dtree, count(pred_dtree) as countp from dfs.`/apps/flights` group by origin, pred_dtree order by origin;
```
**what is the count of predicted and actual  delay/notdelay by origin**
```
> select origin, pred_dtree, count(pred_dtree) as countp,label, count(label) as countl from dfs.`/apps/flights` group by origin, pred_dtree, label order by origin, label, pred_dtree;
```
**what is the count of predicted delay/notdelay by dest**
```
> select dest, pred_dtree, count(pred_dtree) as countp from dfs.`/apps/flights` group by dest, pred_dtree order by dest;
```
**what is the count of predicted delay/notdelay by origin,dest  day of the week,  carrier**
```
> select origin,dest, pred_dtree, count(pred_dtree) as countp from dfs.`/apps/flights` group by origin,dest, pred_dtree order by origin,dest;

> select dofW, pred_dtree, count(pred_dtree) as countp from dfs.`/apps/flights` group by dofW, pred_dtree order by dofW;

> select carrier, pred_dtree, count(pred_dtree) as countp from dfs.`/apps/flights` group by carrier, pred_dtree order by carrier;
```


#### 12. Adding a secondary index to improve queries

Let's now add indices to the payments table.

In a docker container terminal window:

```
$ maprcli table index add -path /apps/flights -index idx_origin -indexedfields 'origin:1'
```
In MapR-DB Shell, try queries on flights origin  and compare with previous query performance:
```
maprdb mapr:> find /apps/flights --where '{ "$eq" : {"origin":"ATL"} }' --f _id,origin,dest,pred_dtree
```
In Drill try 
```
0: jdbc:drill:drillbit=localhost> select _id, pred_dtree from dfs.`/apps/flights` where origin='ATL' and pred_dtree=1.0;

0: jdbc:drill:drillbit=localhost> select _id, pred_dtree , dofW, crsdephour from dfs.`/apps/flights` where origin like 'A%' and dofW=6 and pred_dtree=1.0;

0: jdbc:drill:drillbit=localhost> select  distinct(origin) from dfs.`/apps/flights` ;

```
## Cleaning Up

You can delete the stream and table using the following command from a container terminal:
```
maprcli stream topic delete -path /mapr/maprdemo.mapr.io/apps/stream -topic payment
maprcli table delete -path /mapr/maprdemo.mapr.io/apps/flights

```
## Conclusion

In this example you have learned how to:

* Build and save a spark machine learning model for predicting flight delays
* Publish using the Kafka API  flights data from a JSON file into MapR-ES 
* Consume and enrich the streaming data with the Kafka API, Spark Streaming and the ML model
* Save to the MapR-DB document database using the Spark-DB connector
* Query and Load the JSON data from the MapR-DB document database using the Spark-DB connector and Spark SQL 
* Query the MapR-DB document database using Apache Drill 
* Query the MapR-DB document database using Java and the OJAI library



You can also look at the following examples:

* [mapr-db-60-getting-started](https://github.com/mapr-demos/mapr-db-60-getting-started) to learn Discover how to use DB Shell, Drill and OJAI to query and update documents, but also how to use indexes.
* [Ojai 2.0 Examples](https://github.com/mapr-demos/ojai-2-examples) to learn more about OJAI 2.0 features
* [MapR-DB Change Data Capture](https://github.com/mapr-demos/mapr-db-cdc-sample) to capture database events such as insert, update, delete and react to this events.





