import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object Pagerank {
  def main(args: Array[String]) {
    // Load the edges as a graph

    if (args.length < 1) {
      System.err.println("Usage: PageRank <file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Pagerank")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()


    val graph = GraphLoader.edgeListFile(sc, args(0),numEdgePartitions=16)
    println(spark.time(graph.staticPageRank(20).vertices))

  }
}