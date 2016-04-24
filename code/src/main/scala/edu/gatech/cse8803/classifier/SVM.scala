package edu.gatech.cse8803.classifier

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created by Buckbeak on 4/16/16.
  */
object Svm {

  def classify(training:RDD[LabeledPoint], testing:RDD[LabeledPoint]) : Double = {

        var x = 0;
        var maxAUC = 0.0;
        var maxAUCPR = 0.0;
        var r : RDD[(Double,Double)] = null

        for (x <- 10 to 500)
        {
          val numIterations = x
          val model = SVMWithSGD.train(training, numIterations, 1, 0.01, 1)

          // Clear the default threshold.
          model.clearThreshold()

          // Compute raw scores on the test set.
          val scoreAndLabels = testing.map { point =>
            val score = model.predict(point.features)
            (score, point.label)
          }

          // Get evaluation metrics.
          val metrics = new BinaryClassificationMetrics(scoreAndLabels)
          val auROC = metrics.areaUnderROC()
          val auPR = metrics.areaUnderPR()

          if(auROC > maxAUC) {
            maxAUC = auROC
          }
          if(auPR > maxAUCPR) {
            maxAUCPR = auPR
          }
          println("Area under ROC for " + x + " " + auROC)
          println("Area under PR for " + x + " " + auPR)

        }
        println("Max AUC ROC " +  maxAUC)
        println("Max AUC PR " +  maxAUCPR)


    maxAUC
  }

}
