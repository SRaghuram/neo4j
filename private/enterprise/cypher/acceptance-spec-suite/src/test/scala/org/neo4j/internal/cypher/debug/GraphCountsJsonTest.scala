/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.debug

import org.json4s.DefaultFormats
import org.json4s.Formats
import org.json4s.StringInput
import org.json4s.native.JsonMethods
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class GraphCountsJsonTest extends CypherFunSuite {

  implicit val formats: Formats = DefaultFormats + RowSerializer

  test("Constraint") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "label": "DeprecatedRelyingParty",
        |    "properties": [
        |      "relyingPartyId"
        |    ],
        |    "type": "Uniqueness constraint"
        |}
      """.stripMargin)).extract[Constraint] should be(
      Constraint(
        Some("DeprecatedRelyingParty"),
        None,
        List("relyingPartyId"),
        "Uniqueness constraint"
      )
    )
  }

  test("Relationship Existence Constraint") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "relationshipType": "Foo",
        |    "properties": [
        |      "relyingPartyId"
        |    ],
        |    "type": "Existence constraint"
        |}
      """.stripMargin)).extract[Constraint] should be(
      Constraint(
        None,
        Some("Foo"),
        List("relyingPartyId"),
        "Existence constraint"
      )
    )
  }

  test("index") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "estimatedUniqueSize": 2,
        |    "labels": [
        |        "Person"
        |    ],
        |    "properties": [
        |        "uuid"
        |    ],
        |    "totalSize": 2,
        |    "updatesSinceEstimation": 0
        |}
      """.stripMargin)).extract[Index] should be(
      Index(Seq("Person"), Seq("uuid"), 2, 2, 0)
    )
  }

  test("NodeLCount with Label") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "count": 2,
        |    "label": "Person"
        |}
      """.stripMargin)).extract[NodeCount] should be(
      NodeCount(2, Some("Person"))
    )
  }

  test("NodeCount") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "count": 2,
        |}
      """.stripMargin)).extract[NodeCount] should be(
      NodeCount(2, None)
    )
  }

  test("RelationshipCount") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "count": 5,
        |}
      """.stripMargin)).extract[RelationshipCount] should be(
      RelationshipCount(5, None, None, None)
    )
  }
  test("RelationshipCount with type") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "count": 5,
        |    "relationshipType": "HAS_SSL_CERTIFICATE",
        |}
      """.stripMargin)).extract[RelationshipCount] should be(
      RelationshipCount(5, Some("HAS_SSL_CERTIFICATE"), None, None)
    )
  }
  test("RelationshipCount with type and startLabel") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "count": 5,
        |    "relationshipType": "HAS_SSL_CERTIFICATE",
        |    "startLabel": "RelyingParty"
        |}
      """.stripMargin)).extract[RelationshipCount] should be(
      RelationshipCount(5, Some("HAS_SSL_CERTIFICATE"), Some("RelyingParty"), None)
    )
  }
  test("RelationshipCount with type and endLabel") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "count": 5,
        |    "relationshipType": "HAS_SSL_CERTIFICATE",
        |    "endLabel": "RelyingParty"
        |}
      """.stripMargin)).extract[RelationshipCount] should be(
      RelationshipCount(5, Some("HAS_SSL_CERTIFICATE"), None,  Some("RelyingParty"))
    )
  }

  test("GraphCountData") {
    JsonMethods.parse(StringInput(
      """
        |{"constraints": [{
        |    "label": "SSLCertificate",
        |    "properties": [
        |        "serialNumber"
        |    ],
        |    "type": "Uniqueness constraint"
        |}],
        |"indexes": [{
        |    "estimatedUniqueSize": 4,
        |    "labels": [
        |        "SSLCertificate"
        |    ],
        |    "properties": [
        |        "serialNumber"
        |    ],
        |    "totalSize": 4,
        |    "updatesSinceEstimation": 0
        |}],
        |"nodes": [{
        |    "count": 1,
        |    "label": "VettingProvider"
        |}],
        |"relationships": [{
        |    "count": 1,
        |    "relationshipType": "HAS_GEOLOCATION",
        |    "startLabel": "Address"
        |}]}
      """.stripMargin)).extract[GraphCountData] should be(
      GraphCountData(
        Seq(Constraint(Some("SSLCertificate"), None, Seq("serialNumber"), "Uniqueness constraint")),
        Seq(Index(Seq("SSLCertificate"), Seq("serialNumber"), 4, 4, 0)),
        Seq(NodeCount(1, Some("VettingProvider"))),
        Seq(RelationshipCount(1, Some("HAS_GEOLOCATION"), Some("Address"), None))
      )
    )
  }
}
