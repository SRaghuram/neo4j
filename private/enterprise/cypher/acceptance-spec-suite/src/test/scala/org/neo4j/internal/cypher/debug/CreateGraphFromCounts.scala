/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.debug

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.graphdb.ConstraintViolationException
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType

import scala.collection.mutable.ArrayBuffer

trait CreateGraphFromCounts {
  self : GraphDatabaseTestSupport =>

  /**
   * Creates a graph that is somewhat similar to the statistics given.
   *
   * It creates constraints, indexes and nodes. It guarantees that there are nodes
   * for each label and unlabelled nodes, but it does not guarantee that any count is correct.
   *
   * Currently, no relationships are created.
   */
  def createGraph(graphCountData: GraphCountData): Unit = {
    graph.withTx( tx => {
      for (constraint <- graphCountData.constraints) {
        constraint.`type` match {
          case "Uniqueness constraint" =>
            // Uniqueness constraints can only be on labels, not on relationship types
            val label = constraint.label.getOrElse(throw new IllegalArgumentException(s"Expected a node label in $constraint"))
            val property = getSingleProperty(constraint.properties)
            tx.execute(s"CREATE CONSTRAINT ON (n:$label) ASSERT n.$property IS UNIQUE")
          case "Existence constraint" =>
            (constraint.label, constraint.relationshipType) match {
              case (Some(label), None) =>
                // Node
                val property = getSingleProperty(constraint.properties)
                tx.execute(s"CREATE CONSTRAINT ON (n:$label) ASSERT EXISTS (n.$property)")
              case (None, Some(relType)) =>
                // Relationship
                val property = getSingleProperty(constraint.properties)
                tx.execute(s"CREATE CONSTRAINT ON ()-[n:$relType]-() ASSERT EXISTS (n.$property)")
              case _ =>
                throw new IllegalArgumentException(s"Expected either node or relationship existence constraint, but got: $constraint")
            }
          case "Node Key" =>
            // Node key constraints can only be on labels, not on relationship types
            val label = constraint.label.getOrElse(throw new IllegalArgumentException(s"Expected a node label in $constraint"))
            val properties = propertiesString(constraint.properties)
            tx.execute(s"CREATE CONSTRAINT ON (n:$label) ASSERT $properties IS NODE KEY")
        }
      }

      for (index <- graphCountData.indexes) {
        val label = Label.label(index.labels.head)
        var creator = tx.schema().indexFor(label)
        for (prop <- index.properties)
          creator = creator.on(prop)
        try {
          creator.create()
        } catch {
          // Uniqueness constraints are listed under both constraints and indexes. We create them as a constraint
          // and then fail here when we try to create an index, but a unique index already exists.
          case _: ConstraintViolationException =>
        }
      }
    })

    graph.withTx { tx =>
      val nodeMap = scala.collection.mutable.Map[String, ArrayBuffer[Node]]()
      val uniqueNumber = new AtomicLong()
      for (nodeLabel <- graphCountData.nodes) {
        nodeLabel.label match {
          case None =>
            val labelName = ""
            val nodes = nodeMap.getOrElseUpdate(labelName, new ArrayBuffer)
            for (_ <- 0 until 10) {
              nodes += tx.createNode()
            }
          case Some(labelName) =>
            val label = Label.label(labelName)
            val nodes = nodeMap.getOrElseUpdate(labelName, new ArrayBuffer)
            val relevantConstraints = graphCountData.constraints.filter { const => const.label.contains(labelName) }
            for (_ <- 0L until 10) {
              val node = tx.createNode(label)
              // Create unique property values for all properties involved in constraints
              relevantConstraints.foreach(const => {
                const.properties.foreach(p => node.setProperty(p, uniqueNumber.incrementAndGet()))
              })
              nodes += node
            }
        }
      }

      val groupedRelations = graphCountData.relationships.groupBy(_.relationshipType.getOrElse("__REL"))
      for ((relType, counts) <- groupedRelations) {
        val startNodes = counts.collect { case RelationshipCount(count, _, Some(startLabel), _) => nodeMap(startLabel).take(10) }
        val endNodes = counts.collect { case RelationshipCount(count, _, _, Some(endLabel)) => nodeMap(endLabel).take(10) }

        for {start <- startNodes
             end <- endNodes
             } yield start.zip(end).foreach{ case(s,e) => s.createRelationshipTo(e, RelationshipType.withName(relType))}
      }
    }
  }

  def createGraph(graphCounts: DbStatsRetrieveGraphCountsJSON): Unit = {
    val data = graphCounts.results.head.data.head.row.data
    createGraph(data)
  }

  private def getSingleProperty(properties: Seq[String]) : String = {
    if (properties.size != 1) {
      throw new IllegalArgumentException(s"Expected exactly one property, got: $properties")
    } else {
      properties.head
    }
  }

  private def propertiesString(properties: Seq[String]): String = {
    properties.map(p => s"n.$p").mkString("(", ",", ")")
  }
}
