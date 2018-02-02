package sparkmaprdb

import org.apache.spark._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import com.mapr.db._
import com.mapr.db.spark._
import com.mapr.db.spark.impl._
import com.mapr.db.spark.sql._
import org.apache.log4j.{ Level, Logger }
import com.fasterxml.jackson.annotation.{ JsonIgnoreProperties, JsonProperty }

object QueryFlight {

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

  def main(args: Array[String]) {

    var tableName: String = "/mapr/maprdemo.mapr.io/apps/flights"
    if (args.length == 1) {
      tableName = args(0)
    } else {
      System.out.println("Using hard coded parameters unless you specify the tablename ")
    }
    val spark: SparkSession = SparkSession.builder().appName("querypayment").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    Logger.getLogger("org").setLevel(Level.OFF)

    import spark.implicits._
    // load payment dataset from MapR-DB 
    val fdf: Dataset[FlightwPred] = spark.sparkSession.loadFromMapRDB[FlightwPred](tableName, schema).as[FlightwPred]

    println("Flights from MapR-DB")
    fdf.show

    fdf.createOrReplaceTempView("flight")
    println("what is the count of predicted delay/notdelay for this dstream dataset")
    fdf.groupBy("pred_dtree").count().show()

    println("what is the count of predicted delay/notdelay by scheduled departure hour")
    spark.sql("select crsdephour, pred_dtree, count(pred_dtree) from flight group by crsdephour, pred_dtree order by crsdephour").show
    println("what is the count of predicted delay/notdelay by origin")
    spark.sql("select origin, pred_dtree, count(pred_dtree) from flight group by origin, pred_dtree order by origin").show
    println("what is the count of predicted and actual  delay/notdelay by origin")
    spark.sql("select origin, pred_dtree, count(pred_dtree),label, count(label) from flight group by origin, pred_dtree, label order by origin, label, pred_dtree").show
    println("what is the count of predicted delay/notdelay by dest")
    spark.sql("select dest, pred_dtree, count(pred_dtree) from flight group by dest, pred_dtree order by dest").show
    println("what is the count of predicted delay/notdelay by origin,dest")
    spark.sql("select origin,dest, pred_dtree, count(pred_dtree) from flight group by origin,dest, pred_dtree order by origin,dest").show
    println("what is the count of predicted delay/notdelay by day of the week")
    spark.sql("select dofW, pred_dtree, count(pred_dtree) from flight group by dofW, pred_dtree order by dofW").show
    println("what is the count of predicted delay/notdelay by carrier")
    spark.sql("select carrier, pred_dtree, count(pred_dtree) from flight group by carrier, pred_dtree order by carrier").show

    val lp = fdf.select("label", "pred_dtree")
    val counttotal = fdf.count()
    val label0count = lp.filter($"label" === 0.0).count()
    val pred0count = lp.filter($"pred_dtree" === 0.0).count()
    val label1count = lp.filter($"label" === 1.0).count()
    val pred1count = lp.filter($"pred_dtree" === 1.0).count()

    val correct = lp.filter($"label" === $"pred_dtree").count()
    val wrong = lp.filter(not($"label" === $"pred_dtree")).count()
    val ratioWrong = wrong.toDouble / counttotal.toDouble
    val ratioCorrect = correct.toDouble / counttotal.toDouble
    val truep = lp.filter($"pred_dtree"=== 0.0)
      .filter($"label" === $"pred_dtree").count() / counttotal.toDouble
    val truen = lp.filter($"pred_dtree"=== 1.0)
      .filter($"label" === $"pred_dtree").count() / counttotal.toDouble
    val falsep = lp.filter($"pred_dtree"=== 0.0)
      .filter(not($"label" === $"pred_dtree")).count() / counttotal.toDouble
    val falsen = lp.filter($"pred_dtree"=== 1.0)
      .filter(not($"label" === $"pred_dtree")).count() / counttotal.toDouble

    println("ratio correct ", ratioCorrect)
    println("ratio wrong ", ratioWrong)
    println("correct ", correct)
    println("true positive ", truep)
    println("true negative ", truen)
    println("false positive ", falsep)
    println("false negative ", falsen)
  }
}

