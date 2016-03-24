/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.main

import java.text.SimpleDateFormat

import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.jaccard.Jaccard
import edu.gatech.cse8803.model._
import edu.gatech.cse8803.randomwalk.RandomWalk
import edu.gatech.cse8803.clustering.PowerIterationClustering
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import edu.gatech.cse8803.graphconstruct.GraphLoader
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import scala.collection.mutable
import org.apache.spark.mllib.clustering.LDA


object Main {
  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = createContext
    val sqlContext = new SQLContext(sc)

    /** initialize loading of data */
    val (patient, medication, labResult, diagnostic) = loadRddRawData(sqlContext)

    List("data/sofa.csv", "data/sapsii.csv", "data/saps.csv", "data/apsiii.csv", "data/topics.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _))

    val SOFA = sqlContext.sql( // fix this
      """
        |SELECT hadm_id, sofa
        |FROM sofa
      """.stripMargin)
      .map(r => (r(0).toString, r(1).toString.toInt))


    val SAPSII = sqlContext.sql( // fix this
      """
        |SELECT hadm_id, sapsii
        |FROM sapsii
      """.stripMargin)
      .map(r => (r(0).toString, r(1).toString.toInt))

    val SAPS = sqlContext.sql( // fix this
      """
        |SELECT hadm_id, saps
        |FROM saps
      """.stripMargin)
      .map(r => (r(0).toString, r(1).toString.toInt))

    val APSIII = sqlContext.sql( // fix this
      """
        |SELECT hadm_id, apsiii
        |FROM apsiii
      """.stripMargin)
      .map(r => (r(0).toString, r(1).toString.toInt))

    val topic = sqlContext.sql( // fix this
      """
        |SELECT hadm_id, docs
        |FROM topics
      """.stripMargin)
      .map(r => (r(0).toString, r(1).toString))

    //Topic Modelling Start
    // Load documents from text files, 1 document per file
    val corpus = topic

    // Split each document into a sequence of terms (words)
    val tokenized =
      corpus.map(line => (line._1 , line._2.toLowerCase.split("\\s"))).map(line => (line._1, line._2.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)).toSeq))

    // Choose the vocabulary.
    //   termCounts: Sorted list of (term, termCount) pairs
    val termCounts: Array[(String, Long)] =
      tokenized.flatMap(_._2.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
    //   vocabArray: Chosen vocab (removing common terms)
    val numStopwords = 20
    val vocabArray: Array[String] =
      termCounts.takeRight(termCounts.size - numStopwords).map(_._1)
    //   vocab: Map term -> term index
    val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

    // Convert documents into term count vectors
    val documents: RDD[(Long, Vector)] =
      tokenized.map { case (id, tokens) =>
        val counts = new mutable.HashMap[Int, Double]()
        tokens.foreach { term =>
          if (vocab.contains(term)) {
            val idx = vocab(term)
            counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
          }
        }
        (id.toLong, Vectors.sparse(vocab.size, counts.toSeq))
      }

    // Set LDA parameters
    val numTopics = 50
    val lda = new LDA().setK(numTopics).setMaxIterations(30)

    val ldaModel = lda.run(documents)
    val avgLogLikelihood = ldaModel.logLikelihood / documents.count()

    val ld = ldaModel.topicDistributions.map(line=> (line._1.toString, (1, line._2.toArray.toList)))

//    // Print topics, showing top-weighted 10 terms for each topic.
//    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
//    topicIndices.foreach { case (terms, termWeights) =>
//      println("TOPIC:")
//      terms.zip(termWeights).foreach { case (term, weight) =>
//        println(s"${vocabArray(term.toInt)}\t$weight")
//      }
//      println()
//    }

    val x = patient.map(line => if (line.deathtime.isEmpty) (line.hadmID, 0) else (line.hadmID, 1) ).join(SOFA).join(SAPS).join(SAPSII).join(APSIII).map(r => (r._1, (r._2._1._1._1._1.toDouble, List( r._2._1._1._1._2.toDouble, r._2._1._1._2.toDouble , r._2._1._2.toDouble ,r._2._2.toDouble))))

    val f = x.join(ld).map(line => (line._1 , line._2._1._1, line._2._1._2 ::: line._2._2._2)).map(line => (line._1, line._2, line._3.toArray))

    val dat = f.map(line => LabeledPoint(line._2.toDouble, Vectors.dense(line._3)))


    // Prediction

    val splits = dat.randomSplit(Array(0.7, 0.3), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model 0.733
    val numIterations = 350
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)


//    // Train a RandomForest model. 0.721
//    // Empty categoricalFeaturesInfo indicates all features are continuous.
//    val numClasses = 2
//    val categoricalFeaturesInfo = Map[Int, Int]()
//    val numTrees = 10 // Use more in practice.
//    val featureSubsetStrategy = "auto" // Let the algorithm choose.
//    val impurity = "gini"
//    val maxDepth = 30
//    val maxBins = 32
//
//    val model = RandomForest.trainClassifier(training, numClasses, categoricalFeaturesInfo,
//      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)


    // Train a GradientBoostedTrees model.
    // The defaultParams for Classification use LogLoss by default.
//    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
//    boostingStrategy.numIterations = 10 // Note: Use more iterations in practice.
//    boostingStrategy.treeStrategy.numClasses = 2
//    boostingStrategy.treeStrategy.maxDepth = 30
//    // Empty categoricalFeaturesInfo indicates all features are continuous.
//    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()
//
//    val model = GradientBoostedTrees.train(training, boostingStrategy)
//
//
//    // Evaluate model on test instances and compute test error
//    val labelAndPreds = test.map { point =>
//      val prediction = model.predict(point.features)
//      (prediction, point.label)
//    }
//
//    labelAndPreds.foreach(println)
//
//    // Get evaluation metrics.
//    val metrics = new BinaryClassificationMetrics(labelAndPreds)
//    val auROC = metrics.areaUnderROC()
//
//    println("Area under ROC = " + auROC)




//    val patientGraph = GraphLoader.load( patient, labResult, medication, diagnostic )
//
//    println(Jaccard.jaccardSimilarityOneVsAll(patientGraph, 9))
//    println(RandomWalk.randomWalkOneVsAll(patientGraph, 9))
//
//    val similarities = Jaccard.jaccardSimilarityAllPatients(patientGraph)
//
//    val PICLabels = PowerIterationClustering.runPIC(similarities)
    
    sc.stop()
  }

  def loadRddRawData(sqlContext: SQLContext): (RDD[PatientProperty], RDD[Medication], RDD[LabResult], RDD[Diagnostic]) = {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")

    List("data/PATIENT.csv", "data/LAB.csv", "data/DIAGNOSTIC.csv", "data/MEDICATION.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _))

    val patient = sqlContext.sql( // fix this
      """
        |SELECT subject_id, gender, dob, dod, hadm_id, deathtime
        |FROM PATIENT
      """.stripMargin)
      .map(r => PatientProperty(r(0).toString, r(1).toString, r(2).toString, r(3).toString, r(4).toString, r(5).toString))

    val labResult = sqlContext.sql(
      """
        |SELECT subject_id, date, lab_name, value
        |FROM LAB
        |WHERE value IS NOT NULL and value <> ''
      """.stripMargin)
      .map(r => LabResult(r(0).toString, r(1).toString.toLong, r(2).toString, r(3).toString ))

    val diagnostic = sqlContext.sql(
      """
        |SELECT subject_id, date, code, sequence
        |FROM DIAGNOSTIC
      """.stripMargin)
      .map(r => Diagnostic(r(0).toString, r(1).toString.toLong, r(2).toString, r(3).toString.toInt ))

    val medication = sqlContext.sql(
      """
        |SELECT subject_id, date, med_name
        |FROM MEDICATION
      """.stripMargin)
      .map(r => Medication(r(0).toString, r(1).toString.toLong, r(2).toString))

    (patient, medication, labResult, diagnostic)

  }


  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Three Application", "local")
}
