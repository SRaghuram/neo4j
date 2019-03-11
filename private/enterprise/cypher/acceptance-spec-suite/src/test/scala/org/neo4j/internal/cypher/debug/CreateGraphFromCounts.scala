/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.debug

import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.graphdb.{ConstraintViolationException, Label, Node}

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
  def createGraph(graphCounts: DbStatsRetrieveGraphCountsJSON): Unit = {
    val row = graphCounts.results.head.data.head.row

    graph.inTx {
      for (constraint <- row.data.constraints) {
        constraint.`type` match {
          case "Uniqueness constraint" =>
            // Uniqueness constraints can only be on labels, not on relationship types
            val label = constraint.label.getOrElse(throw new IllegalArgumentException(s"Expected a node label in $constraint"))
            val property = getSingleProperty(constraint.properties)
            graph.execute(s"CREATE CONSTRAINT ON (n:$label) ASSERT n.$property IS UNIQUE")
          case "Existence constraint" =>
            (constraint.label, constraint.relationshipType) match {
              case (Some(label), None) =>
                // Node
                val property = getSingleProperty(constraint.properties)
                graph.execute(s"CREATE CONSTRAINT ON (n:$label) ASSERT EXISTS (n.$property)")
              case (None, Some(relType)) =>
                // Relationship
                val property = getSingleProperty(constraint.properties)
                graph.execute(s"CREATE CONSTRAINT ON ()-[n:$relType]-() ASSERT EXISTS (n.$property)")
              case _ =>
                throw new IllegalArgumentException(s"Expected either node or relationship existence constraint, but got: $constraint")
            }
          case "Node key" =>
            // Node key constraints can only be on labels, not on relationship types
            val label = constraint.label.getOrElse(throw new IllegalArgumentException(s"Expected a node label in $constraint"))
            val properties = propertiesString(constraint.properties)
            graph.execute(s"CREATE CONSTRAINT ON (n:$label) ASSERT $properties IS NODE KEY")
        }
      }

      for (index <- row.data.indexes) {
        val label = Label.label(index.labels.head)
        var creator = graph.schema().indexFor(label)
        for (prop <- index.properties)
          creator = creator.on(prop)
        try {
          creator.create()
        } catch {
          case _: ConstraintViolationException =>
        }
      }
    }

    graph.inTx {
      val nodeMap = scala.collection.mutable.Map[String, ArrayBuffer[Node]]()
      for (nodeLabel <- row.data.nodes) {
        nodeLabel.label match {
          case None =>
            val labelName = ""
            val nodes = nodeMap.getOrElseUpdate(labelName, new ArrayBuffer)
            for (_ <- 0 until 10) {
              // Just create _some_ unlabeled node
              nodes += graph.createNode()
            }
          case Some(labelName) =>
            val label = Label.label(labelName)
            val nodes = nodeMap.getOrElseUpdate(labelName, new ArrayBuffer)
            for (_ <- 0L until nodeLabel.count) {
              nodes += graph.createNode(label)
            }
        }
      }

      // TODO if you ever want to create relationships, here would be a good spot
    }
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
