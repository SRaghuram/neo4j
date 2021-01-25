/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.debug

import java.io.File

import org.json4s.JArray
import org.json4s.CustomSerializer
import org.json4s.DefaultFormats
import org.json4s.FileInput
import org.json4s.Formats
import org.json4s.StringInput
import org.json4s.native.JsonMethods

object GraphCountsJson {

  /**
   * Given a JSON file obtained from the data collector, e.g. via
   * `curl -H accept:application/json -H content-type:application/json -d '{"statements":[{"statement":"CALL db.stats.retrieve(\"GRAPH COUNTS\")"}]}' http://_________:7474/db/neo4j/tx/commit > graphCounts.json`,
   * return a 1-1 representation of the
   * JSON structure in Scala case classes.
   *
   * @param file the file with the JSON contents
   * @return the scala representation
   */
  def parseAsGraphCountData(file: File): GraphCountData = {
    implicit val formats: Formats = DefaultFormats + RowSerializer
    JsonMethods.parse(FileInput(file)).extract[GraphCountData]
  }

  def parseAsGraphCountsJson(file: File): DbStatsRetrieveGraphCountsJSON = {
    implicit val formats: Formats = DefaultFormats + RowSerializer
    JsonMethods.parse(FileInput(file)).extract[DbStatsRetrieveGraphCountsJSON]
  }

  def parse(str: String): DbStatsRetrieveGraphCountsJSON = {
    implicit val formats: Formats = DefaultFormats + RowSerializer
    JsonMethods.parse(StringInput(str)).extract[DbStatsRetrieveGraphCountsJSON]
  }
}

/*
 * The below classes represent the JSON structure from data collector output.
 * If you need to inspect other parts of the JSON that are not represented yet, you have to edit
 * these case classes to support deserialization of these parts.
 */

// These classes represent the general result format from calling a REST endpoint
case class DbStatsRetrieveGraphCountsJSON(errors: Seq[String], results: Seq[Result])
case class Result(columns: Seq[String], data: Seq[Data])
case class Data(row: Row)

// These classes represent the particular format for `db.stats.retrieve("GRAPH COUNTS")`
case class Row(section: String, data: GraphCountData)
case class GraphCountData(constraints: Seq[Constraint],
                          indexes: Seq[Index],
                          nodes: Seq[NodeCount],
                          relationships: Seq[RelationshipCount])

case class Constraint(label: Option[String],
                      relationshipType: Option[String],
                      properties: Seq[String],
                      `type`: String)

case class Index(labels: Seq[String],
                 properties: Seq[String],
                 totalSize: Long,
                 estimatedUniqueSize: Long,
                 updatesSinceEstimation: Long)

case class NodeCount(count: Long,
                     label: Option[String])

case class RelationshipCount(count: Long,
                             relationshipType: Option[String],
                             startLabel: Option[String],
                             endLabel: Option[String])

/**
 * This custom serializer is needed because the format of Row is such that offsets in an array
 * map to particular columns.
 */
case object RowSerializer extends CustomSerializer[Row](format => (
  {
    case JArray(arr) =>
      implicit val formats: DefaultFormats = DefaultFormats
      Row(arr.head.extract[String], arr.last.extract[GraphCountData])
  },
  {
    case _:Row => throw new UnsupportedOperationException("Serialization of GraphCounts is not supported.")
  }))
