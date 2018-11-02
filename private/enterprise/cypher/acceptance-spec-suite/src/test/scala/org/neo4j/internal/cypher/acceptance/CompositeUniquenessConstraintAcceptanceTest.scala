/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Versions.{V3_4, V3_5}
import org.neo4j.internal.cypher.acceptance.comparisonsupport._

class CompositeUniquenessConstraintAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should be able to create and remove single property uniqueness constraint") {

    val testconfiguration = Configs.Version3_4 + Configs.Version3_5
    // When
    executeWith(testconfiguration, "CREATE CONSTRAINT ON (n:Person) ASSERT (n.email) IS UNIQUE")

    // Then
    graph should haveConstraints("UNIQUENESS:Person(email)")

    // When
    executeWith(testconfiguration, "DROP CONSTRAINT ON (n:Person) ASSERT (n.email) IS UNIQUE")

    // Then
    graph should not(haveConstraints("UNIQUENESS:Person(email)"))
  }

  val singlePropertyUniquenessFailConf =
    TestConfiguration(Versions(V3_5, V3_4), Planners.Cost, Runtimes.all)

  test("should fail to to create composite uniqueness constraints") {
    // When

    failWithError(singlePropertyUniquenessFailConf + Configs.Morsel,
      "CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS UNIQUE",
      List("Only single property uniqueness constraints are supported"))

    // Then
    graph should not(haveConstraints("UNIQUENESS:Person(firstname,lastname)"))
  }

  test("should fail to to drop composite uniqueness constraints") {
    // When
    failWithError(singlePropertyUniquenessFailConf - Configs.Cost3_1,
      "DROP CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS UNIQUE",
      List("Only single property uniqueness constraints are supported"))

    // Then
    graph should not(haveConstraints("UNIQUENESS:Person(firstname,lastname)"))
  }
}
