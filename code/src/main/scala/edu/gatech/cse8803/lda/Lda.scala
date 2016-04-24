package edu.gatech.cse8803.lda

/**
  * Created by Buckbeak on 4/16/16.
  */

import edu.gatech.cse8803.model._
import org.apache.spark.mllib.clustering.{LDA, DistributedLDAModel}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import scala.collection.mutable


object Lda {

  def topicModelling(topic: RDD[(String, String)], stopWord: RDD[String]): RDD[(String, (Int, List[Double]))] = {

    // Load documents from text files, 1 document per file
    val corpus = topic

    println("Corpus", corpus.count())

    // Split each document into a sequence of terms (words)
    val tokenized =
      corpus.map(line => (line._1 , line._2.toLowerCase.split("\\s"))).map(line => (line._1, line._2.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)).toSeq))

    // Choose the vocabulary.
    //   termCounts: Sorted list of (term, termCount) pairs
    val termCounts: Array[(String, Long)] =
      tokenized.flatMap(_._2.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)


    //   vocabArray: Chosen vocab (removing common terms)
    val stop = stopWord.collect().toSet
    val initialVocab = termCounts.map(_._1).toSet
    val vocabArray = (initialVocab -- stop).toArray

    println("Intial",initialVocab.size)
    println("After", vocabArray.length)

    //   vocab: Map term -> term index
    val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

    // Convert documents into term count vectors
    val doc: RDD[(Long, Long, Vector)] =
      tokenized.zipWithIndex.map { case ((id, tokens), ids) =>
        val counts = new mutable.HashMap[Int, Double]()
        tokens.foreach { term =>
          if (vocab.contains(term)) {
            val idx = vocab(term)
            counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
          }
        }
        (ids.toLong, id.toLong, Vectors.sparse(vocab.size, counts.toSeq))
      }

    val documents = doc.map(line => (line._1, line._3))
    val hadmZip = doc.map(line => (line._1, line._2))

    println("Documents", documents.count())

    // Set LDA parameters
    val numTopics = 50
    val alpha: Double = 50/numTopics
    val beta: Double = 200.00/vocabArray.length
    val lda = new LDA().setK(numTopics).setMaxIterations(0).setDocConcentration(1 + alpha).setTopicConcentration(1 + beta).run(documents)

    val avgLogLikelihood = lda.logLikelihood / documents.count()

    /*
    // ****Uncomment to save the LDA model and load it.

    lda.save(sc,"data/myLDAModel")
    val sameModel = DistributedLDAModel.load(sc, "data/myLDAModel")
    */



    val x = lda.topicDistributions
    println("Lda", x.count())


    val ld = x.join(hadmZip).map(line => (line._2))
      .map(line=> (line._2.toString, (1, line._1.toArray.toList)))
      .reduceByKey{case((a,b),(c,d)) => ((a + c), (b,d).zipped.map(_ + _))}
      .map(line => (line._1, (1, line._2._2.map(_ / line._2._1))))


    //Print topics, showing top-weighted 10 terms for each topic.
    val topicIndices = lda.describeTopics(maxTermsPerTopic = 10)
    topicIndices.foreach { case (terms, termWeights) =>
      println("TOPIC:")
      terms.zip(termWeights).foreach { case (term, weight) =>
        println(s"${vocabArray(term.toInt)}\t$weight")
      }
      println()
    }

    ld
  }

}
