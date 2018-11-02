/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.util.UUID

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class BuiltInFunctionsAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should generate random UUID") {
    val result = execute("RETURN randomUUID() AS uuid")

    val uuid = result.columnAs[String]("uuid").next()
    UUID.fromString(uuid) should be(a[UUID])
  }
}
