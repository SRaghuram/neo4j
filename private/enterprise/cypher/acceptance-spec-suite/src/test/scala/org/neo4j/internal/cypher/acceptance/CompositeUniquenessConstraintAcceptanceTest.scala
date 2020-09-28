/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class CompositeUniquenessConstraintAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should be able to create and remove single property uniqueness constraint") {

    // When
    executeWith(Configs.All, "CREATE CONSTRAINT ON (n:Person) ASSERT (n.email) IS UNIQUE")

    // Then
    graph should haveConstraints("UNIQUENESS:Person(email)")

    // When
    executeWith(Configs.All, "DROP CONSTRAINT ON (n:Person) ASSERT (n.email) IS UNIQUE")

    // Then
    graph should not(haveConstraints("UNIQUENESS:Person(email)"))
  }

  test("should be able to create named and remove single property uniqueness constraint") {

    // When
    executeWith(Configs.All, "CREATE CONSTRAINT my_constraint ON (n:Person) ASSERT (n.email) IS UNIQUE")

    // Then
    graph should haveConstraints("UNIQUENESS:Person(email)")

    // When
    executeWith(Configs.All, "DROP CONSTRAINT ON (n:Person) ASSERT (n.email) IS UNIQUE")

    // Then
    graph should not(haveConstraints("UNIQUENESS:Person(email)"))
  }

  test("should be able to create and remove named single property uniqueness constraint") {

    // When
    executeWith(Configs.All, "CREATE CONSTRAINT my_constraint ON (n:Person) ASSERT (n.email) IS UNIQUE")

    // Then
    graph should haveConstraints("UNIQUENESS:Person(email)")

    // When
    executeWith(Configs.All, "DROP CONSTRAINT my_constraint")

    // Then
    graph should not(haveConstraints("UNIQUENESS:Person(email)"))
  }

  test("should fail to to create composite uniqueness constraints") {
    // When

    failWithError(Configs.All,
      "CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS UNIQUE",
      "Only single property uniqueness constraints are supported")

    // Then
    graph should not(haveConstraints("UNIQUENESS:Person(firstname,lastname)"))
  }

  test("should fail to to drop composite uniqueness constraints") {
    // When
    failWithError(Configs.All,
      "DROP CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS UNIQUE",
      "Only single property uniqueness constraints are supported")

    // Then
    graph should not(haveConstraints("UNIQUENESS:Person(firstname,lastname)"))
  }
}
