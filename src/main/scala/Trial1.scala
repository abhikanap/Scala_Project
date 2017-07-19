package main.scala
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext
import org.apache.phoenix.spark._
import org.apache.hadoop.hbase.HBaseConfiguration
/**
  * Created by abhikanap on 7/19/2017.
  */
object Trial1
{
  def main(args: Array[String]) {

    val conf = new SparkConf();
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hbaseConfiguration = HBaseConfiguration.create()


    val path = "rms.json"
    val jsondf = sqlContext.read.json(path)
    jsondf.show

    jsondf.saveToPhoenix("RMSJSON", Seq("SRP", "PERIOD", "END_DATE", "RATE", "ACTION", "ID", "TYPE", "START_DATE"), zkUrl = Some("phoenix-server:2181/hbase-unsecure"))


  }
}

