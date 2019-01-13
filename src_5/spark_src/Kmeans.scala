package homework.kmeans

import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object Kmeans {
  def main(args: Array[String]): Unit = {
    val startTime = new Date().getTime()
    val trainDataPath = "hdfs://master:9000/finalhomework/data/kmeans3000000.txt"
    val conf = new SparkConf().setAppName("SVM")
    val sc = new SparkContext(conf)

    val data = sc.textFile(trainDataPath)
    val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

    val numClusters = 6
    val numIterations = 10

    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    val endTime = new Date().getTime()
    val timeCost = endTime - startTime
    val averageTime = timeCost / numIterations
    val WSSSE = clusters.computeCost(parsedData)
    println(s"Errors = $WSSSE")
    println(s"time cost : $timeCost")
    println(s"average time: $averageTime")
  }

}
