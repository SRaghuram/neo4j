/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class UpdateReportingAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {
  test("creating a node gets reported as such") {
    val output = dumpToString("CREATE (:A)")

    output should include("Nodes created: 1")
    output should include("Labels added: 1")
  }
}
