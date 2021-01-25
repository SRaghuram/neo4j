/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite

class SerializationAcceptanceTest extends ExecutionEngineFunSuite {

  // serialization of deleted entities

  test("deleted nodes should be returned marked as such") {
    createNode()

    val query = "MATCH (n) DELETE n RETURN n"

    graph.withTx( tx => {
      val result = tx.execute(query).resultAsString()

      result should include("Node[0]{deleted}")
    } )
  }

  test("non-deleted nodes should be returned as normal") {
    createNode()

    val query = "MATCH (n) RETURN n"

    graph.withTx( tx => {
      val result = tx.execute(query).resultAsString()

      result should not include "deleted"
    } )
  }

  test("non-deleted relationships should be returned as normal") {
    relate(createNode(), createNode(), "T")

    val query = "MATCH ()-[r]->() RETURN r"

    graph.withTx( tx => {
      val result = tx.execute(query).resultAsString()

      result should not include "deleted"
    })
  }

  test("deleted relationships should be returned marked as such") {
    relate(createNode(), createNode(), "T")

    val query = "MATCH ()-[r]->() DELETE r RETURN r"

    graph.withTx( tx => {
      val result = tx.execute(query).resultAsString()

      result should include(":T[0]{deleted}")
    })
  }

  test("returning everything when including deleted entities should work") {
    relate(createNode(), createNode(), "T")

    val query = "MATCH (a)-[r]->(b) DELETE a, r, b RETURN *"

    graph.withTx( tx => {
      val result = tx.execute(query).resultAsString()

      result should include(":T[0]{deleted}")
      result should include("Node[0]{deleted}")
      result should include("Node[1]{deleted}")
    })
  }

  test("returning a deleted path") {
    relate(createNode(), createNode(), "T")

    val query = "MATCH p=(a)-[r]->(b) DELETE p RETURN p"

    graph.withTx( tx => {
      val result = tx.execute(query).resultAsString()

      result should include("[0:T,deleted]")
      result should include("(0,deleted)")
      result should include("(1,deleted)")
    })
  }

  test("returning a deleted path with deleted node") {
    relate(createNode(), createNode(), "T")

    val query = "MATCH p=(a)-[r]->(b) DELETE a, r RETURN p"

    graph.withTx( tx => {
      val result = tx.execute(query).resultAsString()

      result should include("[0:T,deleted]")
      result should include("(0,deleted)")
      result should not include "(1,deleted)"
    })
  }

  test("returning a deleted path with deleted relationship") {
    relate(createNode(), createNode(), "T")

    val query = "MATCH p=(a)-[r]->(b) DELETE r RETURN p"

    graph.withTx( tx => {
      val result = tx.execute(query).resultAsString()

      result should include("[0:T,deleted]")
      result should not include "(0,deleted)"
      result should not include "(1,deleted)"
    })
  }

}
