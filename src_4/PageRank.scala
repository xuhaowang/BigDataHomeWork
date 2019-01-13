package homework.pagerank

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Date

object PageRank {
  def main(args: Array[String]): Unit = {
    var startTime = new Date().getTime
    var endTime1 = 0L
    val inputFile = "hdfs://master:9000/pagerank/page_rank_data.txt"
    val outFile = "hdfs://master:9000//pagerank/result"

    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(inputFile)
    val links = lines.map(line=>{
      val parts = line.split("\\s")
      (parts(0), parts(1))
    }).distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.00)

    for(i <- 1 to 100){
      val contrib = links.leftOuterJoin(ranks).values.flatMap {
        case (urls, rank) => {
          val size = urls.size
          urls.map(url => (url, rank/size))
        }
      }
      ranks = contrib.reduceByKey(_+_).mapValues(0.15+0.85*_)
      if(i == 1){
         endTime1 = new Date().getTime
      }
    }
    var endTime = new Date().getTime
    ranks.sortBy(_._2).collect()
    ranks.saveAsTextFile(outFile)
    println(endTime1 - startTime)
    println(endTime - startTime)
  }

}
