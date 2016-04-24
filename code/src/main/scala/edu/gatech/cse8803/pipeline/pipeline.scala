package edu.gatech.cse8803.pipeline

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD


/**
  * Created by Buckbeak on 4/16/16.
  */
object pipeline {

  def classify(sc:SparkContext, training:RDD[LabeledPoint], testing:RDD[LabeledPoint]) : Double = {

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val lr = new LogisticRegression()

    //Cross Validation

    val pipeline = new Pipeline()
      .setStages(Array(lr))

    val paramGrid = new ParamGridBuilder()
        .addGrid(lr.regParam, Array(1.0, 0.5, 0.3, 0.1, 0.05))
      .addGrid(lr.maxIter, Array(100, 150, 200, 250, 300, 350, 400, 450, 500))
      .build()


    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)

    val cvModel = cv.fit(training.toDF("label", "features"))

    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    val labelAndPreds = cvModel.transform(testing.toDF("id", "features"))
      .map({case Row(a:Double, b:Vector, c:Vector, d:Vector, e:Double) => (a , e)})

    val metrics = new BinaryClassificationMetrics(labelAndPreds)
    val auROC = metrics.areaUnderROC()

    val auPR = metrics.areaUnderPR()

    println("Area under ROC = " + auROC)
    println("Area under PR = " + auPR)


    auROC

  }

}
