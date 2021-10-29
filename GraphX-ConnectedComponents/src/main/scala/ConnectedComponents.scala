import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ConnectedComponents {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.

    if (args.length < 1) {
      System.err.println("Usage: ConnectedComponents <file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("ConnectedComponents")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()

    val graph = GraphLoader.edgeListFile(sc, args(0),numEdgePartitions=16)
    println(spark.time(graph.connectedComponents().vertices))
  }
}