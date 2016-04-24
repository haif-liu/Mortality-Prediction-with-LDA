/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.main

import java.text.SimpleDateFormat

import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model._
import edu.gatech.cse8803.lda.Lda
import edu.gatech.cse8803.classifier.Svm
import edu.gatech.cse8803.pipeline.pipeline
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}



object Main {
  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = createContext
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    /** initialize loading of data */
    val patient = loadRddRawData(sqlContext)

    List("data/sofa.csv", "data/sapsii.csv", "data/saps.csv", "data/apsiii.csv", "data/oasis.csv", "data/notes.csv", "data/notesicurefine.csv",  "data/stop.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _))

    val SOFA = sqlContext.sql( // fix this
      """
        |SELECT hadm_id, icustay_id, sofa
        |FROM sofa
      """.stripMargin)
      .map(r => (r(0).toString, (r(1).toString, r(2).toString.toInt)))

    val SAPSII = sqlContext.sql( // fix this
      """
        |SELECT hadm_id, icustay_id, sapsii
        |FROM sapsii
      """.stripMargin)
      .map(r => (r(0).toString, (r(1).toString, r(2).toString.toInt)))

    val SAPS = sqlContext.sql( // fix this
      """
        |SELECT hadm_id, icustay_id, saps
        |FROM saps
      """.stripMargin)
      .map(r => (r(0).toString, (r(1).toString, r(2).toString.toInt)))

    val APSIII = sqlContext.sql( // fix this
      """
        |SELECT hadm_id, icustay_id, apsiii
        |FROM apsiii
      """.stripMargin)
      .map(r => (r(0).toString, (r(1).toString, r(2).toString.toInt)))

    val OASIS = sqlContext.sql( // fix this
      """
        |SELECT hadm_id, icustay_id, oasis
        |FROM oasis
      """.stripMargin)
      .map(r => (r(0).toString, (r(1).toString, r(2).toString.toInt)))

    val topic = sqlContext.sql( // fix this
      """
        |SELECT hadm_id, docs
        |FROM notes
      """.stripMargin)
      .map(r => (r(0).toString, r(1).toString)).reduceByKey(_+_)

    val topicFilter = sqlContext.sql( // fix this
      """
        |SELECT hadm_id, docs
        |FROM notesicurefine
      """.stripMargin)
      .map(r => (r(0).toString, r(1).toString)).reduceByKey(_+_)

    val stopword = sqlContext.sql( // fix this
      """
        |SELECT stopwords
        |FROM stop
      """.stripMargin)
      .map(r => (r(0).toString))

    /*
    // ****Following code is to generate test set features

    val splits = topic.randomSplit(Array(0.7, 0.3), seed = 11L)
    val train = splits(0)
    val test = splits(1)
    val testFilter = topic.join(topicFilter).map(line => (line._1, line._2._2))

    */

    //Topic Modelling Start
    val ld = Lda.topicModelling(topic, stopword)
    val ldtrain = ld.join(topicFilter).map(line => (line._1, line._2._1))

    /*
    // ****For generating test set
    val ldtest = ld.join(testFilter).map(line => (line._1, line._2._1))
    */


    //Feature Construction
    val p = patient.map(line => if(line.sex.equalsIgnoreCase("M")) (line.hadmID, (line.deathtime,line.age, 1.0)) else (line.hadmID, (line.deathtime, line.age, 0.0)))

    val sofafirst = SOFA.reduceByKey{case((a,b),(c,d)) => if(a <= c) (a,b) else (c,d)}.map(line => (line._1, line._2._2))
    val sapsfirst = SAPS.reduceByKey{case((a,b),(c,d)) => if(a <= c) (a,b) else (c,d)}.map(line => (line._1, line._2._2))
    val sapsiifirst = SAPSII.reduceByKey{case((a,b),(c,d)) => if(a <= c) (a,b) else (c,d)}.map(line => (line._1, line._2._2))
    val apsiiifirst = APSIII.reduceByKey{case((a,b),(c,d)) => if(a <= c) (a,b) else (c,d)}.map(line => (line._1, line._2._2))
    val oasisfirst = OASIS.reduceByKey{case((a,b),(c,d)) => if(a <= c) (a,b) else (c,d)}.map(line => (line._1, line._2._2))

    val basic = p.map(line => if (line._2._1.isEmpty) (line._1, (0, line._2._2, line._2._3)) else (line._1, (1, line._2._2, line._2._3)) )
        .join(sofafirst).join(sapsfirst).join(sapsiifirst).join(apsiiifirst).join(oasisfirst)
      .map(r => (r._1, (r._2._1._1._1._1._1._1.toDouble, List( r._2._1._1._1._1._1._2.toDouble, r._2._1._1._1._1._1._3.toDouble, r._2._1._1._1._1._2.toDouble,  r._2._1._1._1._2.toDouble , r._2._1._1._2.toDouble , r._2._1._2.toDouble))))

    println("Patient",patient.count())
    println("BASIC",basic.count())

    val topicBasic = basic.join(ldtrain)

    /*
    // ****For generating test set

    val topicBasictest = basic.join(ldtest)
    */

    println("Topic After",topicBasic.count())

    val trainadm = topicBasic.map(line => (line._1 , line._2._1._1, line._2._1._2))
      .map(line => (line._1, line._2, line._3.toArray))
      .map(line => LabeledPoint(line._2.toDouble, Vectors.dense(line._3)))
    val traintopic = topicBasic.map(line => (line._1 , line._2._1._1, line._2._2._2))
      .map(line => (line._1, line._2, line._3.toArray))
      .map(line => LabeledPoint(line._2.toDouble, Vectors.dense(line._3)))
    val trainboth = topicBasic.map(line => (line._1 , line._2._1._1, line._2._1._2 ::: line._2._2._2))
      .map(line => (line._1, line._2, line._3.toArray))
      .map(line => LabeledPoint(line._2.toDouble, Vectors.dense(line._3)))

    trainadm.saveAsTextFile("data/featuresTrainadm")
    traintopic.saveAsTextFile("data/featuresTraintopic")
    trainboth.saveAsTextFile("data/featuresTrainCombined")

    /*
    // ****Following code is to generate test set features

    val testadm = topicBasictest.map(line => (line._1 , line._2._1._1, line._2._1._2))
      .map(line => (line._1, line._2, line._3.toArray))
      .map(line => LabeledPoint(line._2.toDouble, Vectors.dense(line._3)))
    val testtopic = topicBasictest.map(line => (line._1 , line._2._1._1, line._2._2._2))
      .map(line => (line._1, line._2, line._3.toArray))
      .map(line => LabeledPoint(line._2.toDouble, Vectors.dense(line._3)))
    val testboth = topicBasictest.map(line => (line._1 , line._2._1._1, line._2._1._2 ::: line._2._2._2))
      .map(line => (line._1, line._2, line._3.toArray))
      .map(line => LabeledPoint(line._2.toDouble, Vectors.dense(line._3)))


    testadm.saveAsTextFile("data/featurestestadm")
    testtopic.saveAsTextFile("data/featurestesttopic")
    testboth.saveAsTextFile("data/featurestestboth")

    */


    // Load and parse the data. Change the folder path to run prediction of the required directory output
    val data = sc.textFile("data/featuresTrainCombined/part-000[0-50]*")
    val reg = "\\[|\\]|\\(|\\)"
    val parsedData = data.map { line =>
      val parts = line.split('[')
      LabeledPoint(parts(0).replaceAll(reg+"|\\,", "").toDouble, Vectors.dense(parts(1).replaceAll(reg, "").split(',').map(_.toDouble)))
    }.cache()

    //Enrichment of LDA Topics
    val enrichData = data.map { line =>
      val parts = line.split('[')
      (parts(0).replaceAll(reg+"|\\,", "").toDouble, parts(1).replaceAll(reg, "").split(',').map(_.toDouble))
    }

    val numerator = enrichData.map(line => if(line._1 == 1.0 ) (1, line._2.toList) else (1, Array.fill[Double](56)(0.0).toList))
      .reduceByKey{case(a,b) => a.zip(b).map(t => t._1 + t._2)}.map(line => line._2).collect().toList(0)
    val denominator = enrichData.map(line => (1, line._2.toList))
      .reduceByKey{case(a,b) => a.zip(b).map(t => t._1 + t._2)}.map(line => line._2).collect().toList(0)

    val enrich = numerator.zip(denominator).map(t => t._1 / t._2)

    println(enrich)

    // Prediction
    val splits = parsedData.randomSplit(Array(0.7, 0.3), seed = 11L)
    val training = splits(0).cache
    val test = splits(1)

    val auroc = pipeline.classify(sc, training, test)

    /*
    // ****For testing SVM Classifier

    val auroc = Svm.classify(training, test)

    */


    sc.stop()
  }

  def loadRddRawData(sqlContext: SQLContext): RDD[PatientProperty] = {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")

    List("data/PATIENT.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _))

    val patient = sqlContext.sql( // fix this
      """
        |SELECT subject_id, gender, dob, age, hadm_id, deathtime
        |FROM PATIENT
      """.stripMargin)
      .map(r => PatientProperty(r(0).toString, r(1).toString, r(2).toString, r(3).toString, r(4).toString, r(5).toString))

    patient

  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Project Application", "local")
}
