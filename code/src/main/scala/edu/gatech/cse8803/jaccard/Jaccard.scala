/**

students: please put your implementation in this file!
  **/
package edu.gatech.cse8803.jaccard

import edu.gatech.cse8803.model._
import edu.gatech.cse8803.model.{EdgeProperty, VertexProperty}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Jaccard {

  def jaccardSimilarityOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: Long): List[Long] = {
    /** 
    Given a patient ID, compute the Jaccard similarity w.r.t. to all other patients. 
    Return a List of patient IDs ordered by the highest to the lowest similarity.
    For ties, random order is okay
    */
    val max_patient_vertex = graph.vertices.filter(line => line._2.isInstanceOf[PatientProperty]).map(_._1).max()

    /** Remove this placeholder and implement your code */
    val patient = graph.collectNeighborIds(EdgeDirection.Out).filter(line => line._1 <= max_patient_vertex).map(line=> (line._1, line._2.toSet))

    val thisPatient = patient.filter(line => line._1 == patientID).map(line => line._2).first()

    patient.filter(line=> line._1 != patientID)
      .map(line => (line._1, jaccard(line._2, thisPatient)))
      .sortBy(_._2).map(_._1)
      .collect().toList.reverse
  }

  def jaccardSimilarityAllPatients(graph: Graph[VertexProperty, EdgeProperty]): RDD[(Long, Long, Double)] = {
    /**
    Given a patient, med, diag, lab graph, calculate pairwise similarity between all
    patients. Return a RDD of (patient-1-id, patient-2-id, similarity) where 
    patient-1-id < patient-2-id to avoid duplications
    */
    val max_patient_vertex = graph.vertices.filter(line => line._2.isInstanceOf[PatientProperty]).map(_._1).max()

    val patient = graph.collectNeighborIds(EdgeDirection.Out).filter(line => line._1 <= max_patient_vertex).map(line=> (line._1, line._2.toSet))

    patient.cartesian(patient).filter(line => line._1._1 < line._2._1)
      .map(line => (line._1._1 , line._2._1, jaccard(line._1._2, line._2._2)))

  }

  def jaccard[A](a: Set[A], b: Set[A]): Double = {
    /** 
    Helper function

    Given two sets, compute its Jaccard similarity and return its result.
    If the union part is zero, then return 0.
    */
    val un = a.union(b).size.toFloat
    val inter = a.intersect(b).size.toFloat

    val jacc = if (un == 0.0) 0.0 else (inter/un)

    /** Remove this placeholder and implement your code */
    jacc
  }
}
