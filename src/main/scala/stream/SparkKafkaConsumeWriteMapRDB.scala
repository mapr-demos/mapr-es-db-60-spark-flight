package stream

import org.apache.spark._
import org.apache.spark.rdd.RDD

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka09.{ ConsumerStrategies, KafkaUtils, LocationStrategies }
import org.apache.spark.streaming.kafka.producer._

import org.apache.kafka.clients.consumer.ConsumerConfig

import com.mapr.db._
import com.mapr.db.spark._
import com.mapr.db.spark.impl._
import com.mapr.db.spark.streaming._
import com.mapr.db.spark.sql._

/*
 Spark streaming consumer 
 consumes from MapR Event Streams transforms CSV to JSON and write to MapR-DB JSON
 */
object SparkKafkaConsumeWriteMapRDB {

  case class FlightwPred(_id: String, dofW: Integer, carrier: String, origin: String,
    dest: String, crsdephour: Integer, crsdeptime: Double, crsarrtime: Double, crselapsedtime: Double,
    label: Double, pred_dtree: Double) extends Serializable

  val schema = StructType(Array(
    StructField("_id", StringType, true),
    StructField("dofW", IntegerType, true),
    StructField("carrier", StringType, true),
    StructField("origin", StringType, true),
    StructField("dest", StringType, true),
    StructField("crsdephour", IntegerType, true),
    StructField("crsdeptime", DoubleType, true),
    StructField("crsarrtime", DoubleType, true),
    StructField("crselapsedtime", DoubleType, true),
    StructField("label", DoubleType, true),
    StructField("pred_dtree", DoubleType, true)
  ))
  def main(args: Array[String]) = {
    var tableName: String = "/mapr/maprdemo.mapr.io/apps/flights"

    var topicp = "/mapr/maprdemo.mapr.io/apps/stream:flightp"

    if (args.length == 2) {
      topicp = args(0)
      tableName = args(1)
    } else {
      System.out.println("Using hard coded parameters unless you specify the consume topic and table. <topic table>   ")
    }

    val groupId = "testgroup"
    val offsetReset = "earliest" //  "latest"
    val pollTimeout = "5000"

    val brokers = "maprdemo:9092" // not needed for MapR Streams, needed for Kafka

    val sparkConf = new SparkConf()
      .setAppName(SparkKafkaConsumeWriteMapRDB.getClass.getName).setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    ssc.sparkContext.setLogLevel("ERROR")
    val topicsSet = topicp.split(",").toSet

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      "spark.kafka.poll.time" -> pollTimeout
    )

    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    val messagesDStream = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, consumerStrategy
    )
    val valuesDStream = messagesDStream.map(_.value())
    
   // valuesDStream.saveToMapRDB(tableName, createTable=false, bulkInsert=true, idFieldPath = "_id")

    valuesDStream.foreachRDD { (rdd: RDD[String], time: Time) =>
      // There exists at least one element in RDD
      if (!rdd.isEmpty) {
        val count = rdd.count
        println("count received " + count)
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._

        import org.apache.spark.sql.functions._
        val df: Dataset[FlightwPred] = spark.read.schema(schema).json(rdd).as[FlightwPred]
        println("dstream dataset")
        df.show
        
        df.saveToMapRDB(tableName, createTable=false, bulkInsert=true, idFieldPath = "_id")

      }
    }

 
    ssc.start()
    ssc.awaitTermination()
    }

}

