/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.debug

import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.graphdb.{ConstraintViolationException, Label, Node, RelationshipType}

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
    graph.inTx {
      for (constraint <- graphCountData.constraints) {
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

      for (index <- graphCountData.indexes) {
        val label = Label.label(index.labels.head)
        var creator = graph.schema().indexFor(label)
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
    }

    graph.inTx {
      val nodeMap = scala.collection.mutable.Map[String, ArrayBuffer[Node]]()
      for (nodeLabel <- graphCountData.nodes) {
        nodeLabel.label match {
          case None =>
            val labelName = ""
            val nodes = nodeMap.getOrElseUpdate(labelName, new ArrayBuffer)
            for (_ <- 0 until 10) {
              nodes += graph.createNode()
            }
          case Some(labelName) =>
            val label = Label.label(labelName)
            val nodes = nodeMap.getOrElseUpdate(labelName, new ArrayBuffer)
            for (_ <- 0L until 10) {
              nodes += graph.createNode(label)
            }
        }
      }

      for(relType <- graphCountData.relationships) {
        val startNodes = relType.startLabel.map(nodeMap).getOrElse(nodeMap("")).take(10)
        val endNodes = relType.endLabel.map(nodeMap).getOrElse(nodeMap("")).take(10)
        val relTypeName = relType.relationshipType.getOrElse("__REL")

        for ((start, end) <- startNodes.zip(endNodes)) {
          start.createRelationshipTo(end, RelationshipType.withName(relTypeName))
        }
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
