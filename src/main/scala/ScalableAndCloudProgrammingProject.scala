//imports
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

import scala.collection.parallel.mutable.ParArray
import scala.collection.parallel.immutable.ParMap


object ScalableAndCloudProgrammingProject {

  def main(args: Array[String]) = {

    //SparkConf for Local execution  
    //val conf = new SparkConf().setAppName("Scalable project").setMaster("local[*]")

    //SparkConf for Cluster execution  
    val conf = new SparkConf().setAppName("Scalable project")

    //SparkContext inizialization
    val sc = new SparkContext(conf)

    //SparkSession inizialization, necessary for DataFrames usage (for the delivery Dataset)
    /*
    val spark = SparkSession
        .builder()
        .appName("Scalable project")
        .config("spark.master", "local[*]") //Local, for cluster mode just comment this line 
        .getOrCreate()
    */

    //Paths on gcloud bucket
    val restaurantPath = "gs://restaurant_recommendation/data/train_full.csv"
    val restaurantPath1 = "gs://restaurant_recommendation/data/orders.csv"
    val booksPath = "gs://book_recommendation/Books.csv"

    /*
     * Reading Restaurant dataset
     */
    /*
    //Train_full dataset, all the combination for a customer and his locations and all possible vendors
    val df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(restaurantPath)

    //Orders dataset, list of orders with customers, vendors and order related features
    val df2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(restaurantPath1)

    //Select only customers, vendors and tags 
    val selected = df1.select("customer_id","id", "vendor_tag_name") 

    //Concatenate customers and vendors in a new column 
    val selectedAll = selected.withColumn("all", concat(col("customer_id"), lit("_"), col("id")))

    val dropped = selectedAll.dropDuplicates("all")

    //Select only customers and vendors 
    val selected1 = df2.select("customer_id","vendor_id")

    //Concatenate customers and vendors in a new column 
    val selectedAll1 = selected1.withColumn("all", concat(col("customer_id"), lit("_"), col("vendor_id")))

    //Merge train_full dataset with orders, mantain only the combinations (customer_vendor) for which effectively an order has been performed
    val merged =dropped.join(selectedAll1, Seq("all"), "inner")

    //Drop columns to get only vendors and tags
    val data = merged.drop("id","customer_id","all").dropDuplicates("vendor_id").na.drop()

    //From DataFrame to RDD
    val rdd = data.rdd

    //From DataFrame Row to Tuple
    val restaurantDatasetFULL = rdd.map((row:Row) => {val v1 = row.getAs[String](0); val v2 = row.getAs[Int](1).toString; (v2,v1)})

    //Dimension of restaurant dataset
    val dimRest = restaurantDatasetFULL.count

    */
    /*
     * Reading Books dataset
     */

    val df3 = sc.textFile(booksPath)

    //Split columns, select ("ISBN","Book-Title")
    val bookData = df3.map(x => x.split(",")).map(x => (x(0), x(1)))

    //Remove the first row (columns names)
    val bookDatasetFULL = bookData.mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it)

    //Dimension of book dataset 
    val dimBook = bookDatasetFULL.count

    //Range(10000,271000,26100) for full dataset
    //Due to memory limitations of the used cluster, the algorithm cannot be executed over a dimension of ~10000 in local and parallel mode  
    //Distributed mode can handle dimensions up to ~36000  
    val sizesBook = Range(3000, 10700, 700)

    //Cicle on dimensions to test the scalability
    for (i <- sizesBook) {

        //Sample of the dataset, RDD repartition (a good value is equal to the number of cores used) 
        val bookDatasetRDD = bookDatasetFULL.sample(true, i.toDouble / dimBook).repartition(8)

        //Dataset of type Array[(String,String)]
        val bookDataset = bookDatasetRDD.collect

        //Dataset of type ParArray[(String,String)]
        val bookDatasetPAR = bookDataset.par

        //Execution Times
        println("\n\nExecution time (local)--Dim[" + i + "]:\t"+ tfidfRecommender.getComputationTime_local(bookDataset, "Books") + "ms\n")
        println("\n\nExecution time (parallel)--Dim[" + i +"]:\t"+ tfidfRecommender.getComputationTime_parallel(bookDatasetPAR, "Books") + "ms\n")
        println("\n\nExecution time (distributed)--Dim[" + i + "]:\t" + tfidfRecommender.getComputationTime_distributed(bookDatasetRDD, "Books") + "ms\n")

        //Example recommendation
        /*
        val example = tfidfRecommender.getTestRecommendation(bookDataset, "Books")

        println("Recommendation for book : " + example._2 + "\n")

        example._1.foreach { case (isbn, value) =>
            println(s"($isbn, $value)")
        }
        */
    }

/*
    //Range(10,97,7) for full Dataset
    val sizesRest = Range(90,97,7)

    //Cicle on dimensions to test the scalability
    for (i <- sizesRest) {
        //Dataset of type RDD[(String,String)]
        val restaurantDatasetRDD = restaurantDatasetFULL.sample(true, i.toDouble / dimRest).repartition(8)

        //Dataset of type Array[(String,String)]
        val restaurantDataset = restaurantDatasetRDD.collect

        //Dataset of type ParArray[(String,String)]
        val restaurantDatasetPAR = restaurantDataset.par

        //Execution Times
        println("\n\nExecution time (local)--Dim[" + i + "]:\t"+ tfidfRecommender.getComputationTime_local(restaurantDataset, "Restaurant") + "ms\n")
        println("\n\nExecution time (parallel)--Dim[" + i +"]:\t"+ tfidfRecommender.getComputationTime_parallel(restaurantDatasetPAR, "Restaurant") + "ms\n")
        println("\n\nExecution time (distributed)--Dim[" + i + "]:\t" + tfidfRecommender.getComputationTime_distributed(restaurantDatasetRDD, "Restaurant") + "ms\n")

        //Example recommendation
        val example = tfidfRecommender.getTestRecommendation(restaurantDataset, "Restaurant")

        println("Recommendation for restaurant : " + example._2 + "\n")

        example._1.foreach { case (restaurant, value) =>
        println(s"($restaurant, $value)")
        }
    }
*/
  }
}

