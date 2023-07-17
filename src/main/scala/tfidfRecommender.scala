//imports
import org.apache.spark.rdd.RDD
import scala.collection.parallel.mutable.ParArray
import scala.collection.parallel.immutable.ParMap

object tfidfRecommender {

  /**
   * Term Frequency calculation, same for all versions
   *
   * @param document string on which calculate TF 
   * @param regex    regular expression for split the string
   * @return         map 'word' -> 'tf_value'
   */
  private def calculateTF(document: String, regex: String): Map[String, Double] = {
      val docTokens = document.split(regex)
      val totalTokens = docTokens.size.toDouble
      docTokens.groupBy(identity).mapValues(_.size / totalTokens)
  }  

  
  /**
   * TFIDF calculation, local version
   *
   * @param documents array of couples ('documentID', 'document')
   * @param regex     regular expression for split the documents
   * @return          array of couples ('documentID', map 'word' -> 'tfidf_value') 
   */
  private def calculateTFIDF_local(documents: Array[(String, String)], regex: String = " "): Array[(String, Map[String, Double])] = {
    
    //Inverse Document Frequency calculation
    def calculateIDF(documents: Array[(String, String)]): Map[String, Double] = {

      val numDocs = documents.size
      val tokenCounts = documents.flatMap { case (id, text) => text.split(regex) }.map(token => (token, 1)).groupBy(identity).mapValues(_.size).toArray.map { case ((first, second), value) => (first, value) }
      val idfValues = tokenCounts.map { case (token, count) => (token, math.log10(numDocs.toDouble / count)) }.toMap[String, Double]

      idfValues
    }

    val idfValues = calculateIDF(documents)

    //TF-IDF calculation
    val tfidfValues = documents.map { case (docId, text) =>
      val termFrequencies = calculateTF(text, regex)
      val tfidf = termFrequencies.map { case (term, tf) => (term, tf * idfValues.getOrElse(term, 0.0)) }
      (docId, tfidf)
    }

    tfidfValues
  }


    /**
    * TFIDF calculation, parallel version
    *
    * @param document parArray of couples ('documentID', 'document')
    * @param regex    regular expression for split the documents
    * @return         parArray of couples ('documentID', map 'word' -> 'tfidf_value') 
    */
  private def calculateTFIDF_parallel(documents: ParArray[(String, String)], regex: String = " "): ParArray[(String, Map[String, Double])] = {

    //Inverse Document Frequency calculation
    def calculateIDF(documents: ParArray[(String, String)]): Map[String, Double] = {
      val numDocs = documents.size
      val tokenCounts = documents.flatMap { case (id, text) => text.split(regex) }.map(token => (token, 1)).groupBy(identity).mapValues(_.size).toArray.map { case ((first, second), value) => (first, value) }
      val idfValues = tokenCounts.map { case (token, count) => (token, math.log10(numDocs.toDouble / count)) }.toMap[String, Double]

      idfValues
    }

    val idfValues = calculateIDF(documents)

    //TF-IDF calculation
    val tfidfValues = documents.map { case (docId, text) =>
      val termFrequencies = calculateTF(text, regex)
      val tfidf = termFrequencies.map { case (term, tf) => (term, tf * idfValues.getOrElse(term, 0.0)) }
      (docId, tfidf)
    }

    tfidfValues
  }

    /**
    * TFIDF calculation, distributed version
    *
    * @param document RDD of couples ('documentID', 'document')
    * @param regex    regular expression for split the documents
    * @return         RDD of couples ('documentID', map 'word' -> 'tfidf_value') 
    */
    private def calculateTFIDF(documents: RDD[(String, String)], regex: String = " "): RDD[(String, Map[String, Double])] = {

    //Inverse Document Frequency calculation
    def calculateIDF(documents: RDD[(String, String)]): Map[String, Double] = {
      val numDocs = documents.count()
      val tokenCounts = documents.flatMap { case (docId, text) => text.split(regex) }.map(token => (token, 1)).reduceByKey(_ + _)
      val idfValues = tokenCounts.map { case (token, count) => (token, math.log10(numDocs.toDouble / count)) }.collect().toMap[String, Double]
      idfValues
    }

    val idfValues = calculateIDF(documents)

    //TF-IDF calculation
    val tfidfValues = documents.map { case (docId, text) =>
      val termFrequencies = calculateTF(text, regex)
      val tfidf = termFrequencies.map { case (term, tf) => (term, tf * idfValues.getOrElse(term, 0.0)) }
      (docId, tfidf)
    }

    tfidfValues
  }

  //Vectors Dot Product
  private def dotProduct(vector1: Map[String, Double], vector2: Map[String, Double]): Double = {
    vector1.foldLeft(0.0) { case (acc, (key, value)) =>
      acc + value * vector2.getOrElse(key, 0.0)
    }
  }
  
  //Magnitude of a vector 
  private def magnitude(vector: Map[String, Double]): Double = {
    math.sqrt(vector.values.map(value => value * value).sum)
  }
  
  //Cosine Similarity between two vectors 
  private def cosineSimilarity(vector1: Map[String, Double], vector2: Map[String, Double]): Double = {
    dotProduct(vector1, vector2) / (magnitude(vector1) * magnitude(vector2))
  }

    /**
    * TFIDF calculation, local version
    *
    * @param document RDD of couples ('documentID', 'document')25
    * @param regex    regular expression for split the documents
    * @return         RDD of couples ('documentID', map 'word' -> 'tfidf_value') 
    */
  def computeCosineSimilarity_local(dataset: Array[(String, Map[String, Double])]): Array[((String, String), Double)] = {
    dataset.flatMap(x => dataset.map(y => (x, y)))
      .filter { case ((doc1, _), (doc2, _)) => doc1 < doc2 }
      .flatMap { case ((doc1, vector1), (doc2, vector2)) =>
        val similarity = cosineSimilarity(vector1, vector2)
        if (similarity != 0.0) Some(((doc1, doc2), similarity)) else None
      }
  }

  //Retrieve a recommendation for an example document, local version
  def getRecommendation_local(id: String, similarities: Array[((String, String), Double)]): Array[(String, Double)] = {
    val matches = similarities.flatMap { case ((id1, id2), value) => if (id1 == id) Some(id2, value) else if (id2 == id) Some(id1, value) else None }
    val sortedMatches = matches.sortBy { case (_, similarity) => -similarity }
    sortedMatches.take(10)
  }

  //Cosine Similarity between each document, parallel version
  def computeCosineSimilarity_parallel(dataset: ParArray[(String, Map[String, Double])]): ParArray[((String, String), Double)] = {
    dataset.flatMap(x => dataset.map(y => (x, y)))
      .filter { case ((doc1, _), (doc2, _)) => doc1 < doc2 }
      .flatMap { case ((doc1, vector1), (doc2, vector2)) =>
        val similarity = cosineSimilarity(vector1, vector2)
        if (similarity != 0.0) Some(((doc1, doc2), similarity)) else None
      }
  }

  //Retrieve a recommendation for an example document, parallel version
  def getRecommendation_parallel(id: String, similarities: ParArray[((String, String), Double)]): Array[(String, Double)] = {
    val matches = similarities.flatMap { case ((id1, id2), value) => if (id1 == id) Some(id2, value) else if (id2 == id) Some(id1, value) else None }
    val sortedMatches = matches.toArray.sortBy { case (_, similarity) => -similarity }
    sortedMatches.take(10)
  }


  //Cosine Similarity between each document, distributed version
  def computeCosineSimilarity(rdd: RDD[(String, Map[String, Double])]): RDD[((String, String), Double)] = {
    rdd.cartesian(rdd)
      .filter { case ((doc1, _), (doc2, _)) => doc1 < doc2 }
      .flatMap { case ((doc1, vector1), (doc2, vector2)) =>
        val similarity = cosineSimilarity(vector1, vector2)
        if (similarity != 0.0) Some(((doc1, doc2), similarity)) else None
      }
  }

  //Retrieve a recommendation for an example document, distributed version
  def getRecommendation(id: String, similarities: RDD[((String, String), Double)]): Array[(String, Double)] = {
    val matches = similarities.flatMap { case ((id1, id2), value) => if (id1 == id) Some(id2, value) else if (id2 == id) Some(id1, value) else None }
    val sortedMatches = matches.sortBy { case (_, similarity) => -similarity }
    sortedMatches.take(10)
  }
  
  //Test function, inputs: the dataset, a string identifying if the dataset is the "Books" one or the "Delivery"
  def getTestRecommendation(documents: Array[(String, String)], dataset: String) = {
    val example = documents(0)._1

    if (dataset.equals("Books")) 
        (getRecommendation_local(example, computeCosineSimilarity_local(calculateTFIDF_local(documents))), example)
    else
        (getRecommendation_local(example, computeCosineSimilarity_local(calculateTFIDF_local(documents, ","))), example)
  }


  //Get execution times, inputs: the dataset, a string identifying if the dataset is the "Books" one or the "Delivery"   
  def getComputationTime_local(documents: Array[(String, String)], dataset: String): (Double, Double) = {

    val exampleID = documents(0)._1

    var t0, t1, t2, t3 = 0.0;

    if (dataset.equals("Books")) {

      t0 = System.nanoTime()

      val idfValues = calculateTFIDF_local(documents)

      t1 = System.nanoTime()

      t2 = System.nanoTime()

      getRecommendation_local(exampleID, computeCosineSimilarity_local(idfValues))

      t3 = System.nanoTime()

    } else {

      t0 = System.nanoTime()
      
      val idfValues = calculateTFIDF_local(documents, ",")

      t1 = System.nanoTime()
      
      t2 = System.nanoTime()

      getRecommendation_local(exampleID, computeCosineSimilarity_local(idfValues))
    
      t3 = System.nanoTime()
    }

    ((t1 - t0) / 1000000, (t3 - t2) / 1000000)
  }

  def getComputationTime_parallel(documents: ParArray[(String, String)], dataset: String): (Double, Double) = {

    val exampleID = documents(0)._1

    var t0, t1, t2, t3 = 0.0;

    if (dataset.equals("Books")) {

      t0 = System.nanoTime()

      val idfValues = calculateTFIDF_parallel(documents)

      t1 = System.nanoTime()

      t2 = System.nanoTime()

      getRecommendation_parallel(exampleID, computeCosineSimilarity_parallel(idfValues))

      t3 = System.nanoTime()

    } else {

      t0 = System.nanoTime()

      val idfValues = calculateTFIDF_parallel(documents, ",")

      t1 = System.nanoTime()

      t2 = System.nanoTime()

      getRecommendation_parallel(exampleID, computeCosineSimilarity_parallel(idfValues))

      t3 = System.nanoTime()
    }

    ((t1 - t0) / 1000000, (t3 - t2) / 1000000)
  }

  def getComputationTime_distributed(documents: RDD[(String, String)], dataset: String): (Double, Double) = {

    val exampleID = documents.first._1

    var t0, t1, t2, t3 = 0.0;

    if (dataset.equals("Books")) {

      t0 = System.nanoTime()

      val idfValues = calculateTFIDF(documents)

      t1 = System.nanoTime()


      t2 = System.nanoTime()

      getRecommendation(exampleID, computeCosineSimilarity(idfValues))

      t3 = System.nanoTime()


    } else {

      t0 = System.nanoTime()
      
      val idfValues = calculateTFIDF(documents, ",")

      t1 = System.nanoTime()
      
      t2 = System.nanoTime()

      getRecommendation(exampleID, computeCosineSimilarity(idfValues))

      t3 = System.nanoTime()
    }

    ((t1 - t0) / 1000000, (t3 - t2) / 1000000)
  }

}