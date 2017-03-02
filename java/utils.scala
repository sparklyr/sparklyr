package SparkWorker

import org.apache.spark._
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SQLContext.implicits._

object Utils {
  def rddToDF(rdd: WorkerRDD) : DataFrame = {
    rdd.toDF()
  }
}
