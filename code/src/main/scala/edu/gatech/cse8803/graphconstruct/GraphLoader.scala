/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.graphconstruct

import edu.gatech.cse8803.model._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object GraphLoader {
  /** Generate Bipartite Graph using RDDs
    *
    * @input: RDDs for Patient, LabResult, Medication, and Diagnostic
    * @return: Constructed Graph
    *
    * */
  def load(patients: RDD[PatientProperty], labResults: RDD[LabResult],
           medications: RDD[Medication], diagnostics: RDD[Diagnostic]): Graph[VertexProperty, EdgeProperty] = {

    /** HINT: See Example of Making Patient Vertices Below */
    val vertexPatient: RDD[(VertexId, VertexProperty)] = patients
      .map(patient => (patient.patientID.toLong, patient.asInstanceOf[VertexProperty]))

    var startDiag = vertexPatient.map(_._1).max() + 1
    val diagnosticVertexIdRDD = diagnostics.
      map(_.icd9code).
      distinct.
      zipWithIndex.
      map{case(icd9code, zeroBasedIndex) =>
        (icd9code, zeroBasedIndex + startDiag)}

    val diagnostic2VertexId = diagnosticVertexIdRDD.collect.toMap

    val diagnosticVertex = diagnosticVertexIdRDD.
      map{case(icd9code, index) => (index, DiagnosticProperty(icd9code))}.
      asInstanceOf[RDD[(VertexId, VertexProperty)]]


    var startLab = diagnostic2VertexId.size + startDiag
    val labVertexIdRDD = labResults.
      map(_.labName).
      distinct.
      zipWithIndex.
      map{case(testName, zeroBasedIndex) =>
        (testName, zeroBasedIndex + startLab)}

    val lab2VertexId = labVertexIdRDD.collect.toMap

    val labVertex = labVertexIdRDD.
      map{case(testName, index) => (index, LabResultProperty(testName))}.
      asInstanceOf[RDD[(VertexId, VertexProperty)]]


    var startDrug = lab2VertexId.size + startLab
    val drugVertexIdRDD = medications.
      map(_.medicine).
      distinct.
      zipWithIndex.
      map{case(medicine, zeroBasedIndex) =>
        (medicine, zeroBasedIndex + startDrug)}

    val drug2VertexId = drugVertexIdRDD.collect.toMap

    val drugVertex = drugVertexIdRDD.
      map{case(medicine, index) => (index, MedicationProperty(medicine))}.
      asInstanceOf[RDD[(VertexId, VertexProperty)]]



    val vertices = vertexPatient.union(diagnosticVertex).union(labVertex).union(drugVertex)

    /** HINT: See Example of Making PatientPatient Edges Below
      *
      * This is just sample edges to give you an example.
      * You can remove this PatientPatient edges and make edges you really need
      * */

    ///////////// Patient and Diag ////////////////

      val patientdiag = diagnostics
      .map{ line => ((line.patientID, line.icd9code), line)}
      .reduceByKey((x, y) => {if ((x.date) > (y.date)) x else y})

    val edgePatientDiagnostic: RDD[Edge[EdgeProperty]] = patientdiag
      .map({line =>
        Edge(line._1._1.toLong, diagnostic2VertexId.get(line._1._2).get , new PatientDiagnosticEdgeProperty(line._2))
      })

    val edgeDiagnosticPatient: RDD[Edge[EdgeProperty]] = patientdiag
      .map({line =>
        Edge(diagnostic2VertexId.get(line._1._2).get, line._1._1.toLong, new PatientDiagnosticEdgeProperty(line._2))
      })


    ///////////// Patient and Lab ////////////////


    val patientlab = labResults
      .map{ line => ((line.patientID, line.labName), line)}
      .reduceByKey((x, y) => {if ((x.date) > (y.date)) x else y})


    val edgePatientLab: RDD[Edge[EdgeProperty]] = patientlab
      .map({line =>
        Edge(line._1._1.toLong, lab2VertexId.get(line._1._2).get , new PatientLabEdgeProperty(line._2))
      })

    val edgeLabPatient: RDD[Edge[EdgeProperty]] = patientlab
      .map({line =>
        Edge(lab2VertexId.get(line._1._2).get, line._1._1.toLong , new PatientLabEdgeProperty(line._2))
      })

    ///////////// Patient and Medication ////////////////

    val patientdrug = medications
      .map{ line => ((line.patientID, line.medicine), line)}
      .reduceByKey((x, y) => {if ((x.date) > (y.date)) x else y})

    val edgePatientDrug: RDD[Edge[EdgeProperty]] = patientdrug
      .map({line =>
        Edge(line._1._1.toLong, drug2VertexId.get(line._1._2).get , new PatientMedicationEdgeProperty(line._2))
      })

    val edgeDrugPatient: RDD[Edge[EdgeProperty]] = patientdrug
      .map({line =>
        Edge(drug2VertexId.get(line._1._2).get ,line._1._1.toLong,  new PatientMedicationEdgeProperty(line._2))
      })

    //////////////////////////////////////

    val edges = edgePatientDiagnostic.union(edgeDiagnosticPatient).union(edgePatientLab).union(edgeLabPatient).union(edgePatientDrug).union(edgeDrugPatient)

    // Making Graph
    val graph: Graph[VertexProperty, EdgeProperty] = Graph(vertices, edges)

    graph
  }
}
