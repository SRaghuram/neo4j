/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}

import scala.language.postfixOps

class UnsupportedFeaturesAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("from graph") {
    val query = "FROM GRAPH foo.bar MATCH (a)-->() RETURN a"
    failWithError(Configs.All, query, List("The `FROM GRAPH` clause is not available in this implementation of Cypher due to lack of support for multiple graphs."))
  }

  test("return graph") {
    val query = "WITH $param AS foo MATCH ()--() RETURN GRAPH"
    failWithError(Configs.All, query, List("The `RETURN GRAPH` clause is not available in this implementation of Cypher due to lack of support for multiple graphs."))
  }

  test("construct graph") {
    val query = "MATCH (a) CONSTRUCT ON foo.bar CLONE a CREATE (a)-[:T {prop: a.prop}]->(:X) RETURN 1 AS a"
    failWithError(Configs.All, query, List("The `CONSTRUCT` clause is not available in this implementation of Cypher due to lack of support for multiple graphs."))
  }

  test("create graph") {
    val query = "CATALOG CREATE GRAPH foo { RETURN GRAPH }"
    failWithError(Configs.All, query, List("The `CATALOG CREATE GRAPH` clause is not available in this implementation of Cypher due to lack of support for multiple graphs."))
  }

  test("delete graph") {
    val query = "CATALOG DROP GRAPH foo"
    failWithError(Configs.All, query, List("The `CATALOG DROP GRAPH` clause is not available in this implementation of Cypher due to lack of support for multiple graphs."))
  }

  test("equivalence operator") {
    val query = "RETURN 1 ~ 2"
    failWithError(Configs.All, query, List("`~` (equivalence) is a Cypher 10 feature and is not available in this implementation of Cypher."))
  }
}
