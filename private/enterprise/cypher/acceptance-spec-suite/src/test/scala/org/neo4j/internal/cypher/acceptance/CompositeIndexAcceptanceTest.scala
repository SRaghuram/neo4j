/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.Values
import org.scalatest.exceptions.TestFailedException
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetTime
import java.time.ZoneOffset
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
 * These tests are testing the actual index implementation, thus they should all check the actual result.
 * If you only want to verify that plans using indexes are actually planned, please use
 * [[org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanningIntegrationTest]]
 */
class CompositeIndexAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should succeed in creating and deleting composite index") {
    // When
    graph.createIndex("Person", "name")
    graph.createIndex("Person", "name", "surname")

    // Then
    graph should haveIndexes(":Person(name)")
    graph should haveIndexes(":Person(name)", ":Person(name,surname)")

    // When
    executeWith(Configs.All, "DROP INDEX ON :Person(name , surname)")

    // Then
    graph should haveIndexes(":Person(name)")
    graph should not(haveIndexes(":Person(name,surname)"))
  }

  test("should use composite index when all predicates are present") {
    // Given
    graph.createIndex("User", "name", "surname")
    val n1 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Soap"), "User")
    createLabeledNode(Map("name" -> "Joe", "surname" -> "Smoke"), "User")
    createLabeledNode(Map("name" -> "Jake", "surname" -> "Soap"), "User")

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:User) WHERE n.surname = 'Soap' AND n.name = 'Joe' RETURN n",
                             planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek")
      }))

    // Then
    result.toComparableResult should equal(List(Map("n" -> n1)))
  }

  test("should use named composite index when all predicates are present") {
    // Given
    graph.createIndexWithName("user_index", "User", "name", "surname")
    val n1 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Soap"), "User")
    createLabeledNode(Map("name" -> "Joe", "surname" -> "Smoke"), "User")
    createLabeledNode(Map("name" -> "Jake", "surname" -> "Soap"), "User")

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:User) WHERE n.surname = 'Soap' AND n.name = 'Joe' RETURN n",
                             planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek")
          .containingArgumentForIndexPlan("n", "User", Seq("name", "surname"))
      }))

    // Then
    result.toComparableResult should equal(List(Map("n" -> n1)))
  }

  test("should not use composite index when not all predicates are present") {
    // Given
    graph.createIndex("User", "name", "surname")
    createLabeledNode(Map("name" -> "Joe", "surname" -> "Soap"), "User")
    createLabeledNode(Map("name" -> "Joe", "surname" -> "Smoke"), "User")
    val n3 = createLabeledNode(Map("name" -> "Jake", "surname" -> "Soap"), "User")
    for (_ <- 1 to 100) {
      createLabeledNode(Map("name" -> "Joe"), "User")
      createLabeledNode(Map("surname" -> "Soap"), "User")
    }

    // When
    val result = executeWith(Configs.All, "MATCH (n:User) WHERE n.name = 'Jake' RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should not(includeSomewhere.aPlan("NodeIndexSeek"))
      }))

    // Then
    result.toComparableResult should equal(List(Map("n" -> n3)))
  }

  test("should use composite index when all predicates are present even in competition with other single property indexes with similar cardinality") {
    // Given
    graph.createIndex("User", "name")
    graph.createIndex("User", "surname")
    graph.createIndex("User", "name", "surname")
    val n1 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Soap"), "User")
    createLabeledNode(Map("name" -> "Joe", "surname" -> "Smoke"), "User")
    createLabeledNode(Map("name" -> "Jake", "surname" -> "Soap"), "User")
    for (_ <- 1 to 100) {
      createLabeledNode("User")
      createLabeledNode("User")
    }

    resampleIndexes()

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:User) WHERE n.surname = 'Soap' AND n.name = 'Joe' RETURN n",
                             planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek")
          .containingArgumentForIndexPlan("n", "User", Seq("name", "surname"))
        plan should not(includeSomewhere.aPlan("NodeIndexSeek")
          .containingArgumentForIndexPlan("n", "User", Seq("name")))
        plan should not(includeSomewhere.aPlan("NodeIndexSeek")
          .containingArgumentForIndexPlan("n", "User", Seq("surname")))
      }))

    // Then
    result.toComparableResult should equal(List(Map("n" -> n1)))
  }

  test("should use named composite index when all predicates are present even in competition with other named single property indexes with similar cardinality") {
    // Given
    graph.createIndexWithName("name_index", "User", "name")
    graph.createIndexWithName("surname_index", "User", "surname")
    graph.createIndexWithName("composite_index", "User", "name", "surname")
    val n1 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Soap"), "User")
    createLabeledNode(Map("name" -> "Joe", "surname" -> "Smoke"), "User")
    createLabeledNode(Map("name" -> "Jake", "surname" -> "Soap"), "User")
    for (_ <- 1 to 100) {
      createLabeledNode("User")
      createLabeledNode("User")
    }

    resampleIndexes()

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:User) WHERE n.surname = 'Soap' AND n.name = 'Joe' RETURN n",
                             planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
       plan should includeSomewhere.aPlan("NodeIndexSeek")
         .containingArgumentForIndexPlan("n", "User", Seq("name", "surname"))
       plan should not(includeSomewhere.aPlan("NodeIndexSeek")
         .containingArgumentForIndexPlan("n", "User", Seq("name")))
       plan should not(includeSomewhere.aPlan("NodeIndexSeek")
         .containingArgumentForIndexPlan("n", "User", Seq("surname")))
      }))

    // Then
    result.toComparableResult should equal(List(Map("n" -> n1)))
  }

  test("should not use composite index when index hint for alternative index is present") {
    // Given
    graph.createIndex("User", "name")
    graph.createIndex("User", "surname")
    graph.createIndex("User", "name", "surname")
    val n1 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Soap"), "User")
    createLabeledNode(Map("name" -> "Joe", "surname" -> "Smoke"), "User")
    createLabeledNode(Map("name" -> "Jake", "surname" -> "Soap"), "User")
    for (_ <- 1 to 100) {
      createLabeledNode("User")
      createLabeledNode("User")
    }

    // When
    val result = executeWith(Configs.All,
      """
        |MATCH (n:User)
        |USING INDEX n:User(surname)
        |WHERE n.surname = 'Soap' AND n.name = 'Joe'
        |RETURN n
        |""".stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should not(includeSomewhere.aPlan("NodeIndexSeek")
          .containingArgumentForIndexPlan("n", "User", Seq("name", "surname")))
        plan should not(includeSomewhere.aPlan("NodeIndexSeek")
          .containingArgumentForIndexPlan("n", "User", Seq("name")))
        plan should includeSomewhere.aPlan("NodeIndexSeek")
          .containingArgumentForIndexPlan("n", "User", Seq("surname"))
      }))

    // Then
    result.toComparableResult should equal(List(Map("n" -> n1)))
  }

  test("should use composite index for UniqueIndexSeek") {
    graph.createNodeKeyConstraint("User", "name", "surname")
    val n1 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Soap"), "User")
    val n2 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Smoke"), "User")
    val n3 = createLabeledNode(Map("name" -> "Jake", "surname" -> "Soap"), "User")

    Seq(
      ("n.name = 'Joe' AND n.surname = 'Soap'", false, false, "", Set(Map("n" -> n1))),
      ("n.name = 'Joe' AND n.surname STARTS WITH 'So'", false, false, "", Set(Map("n" -> n1))),
      ("n.name >= 'Jo' AND n.surname STARTS WITH 'So'", false, true, ".*cache\\[n\\.surname\\] STARTS WITH .*", Set(Map("n" -> n1))),
      ("n.name <= 'Je' AND exists(n.surname)", false, false, "", Set(Map("n" -> n3))),
      ("exists(n.name) AND n.surname > 'S'", true, true, ".*cache\\[n\\.surname\\] > .*", Set(Map("n" -> n1), Map("n" -> n2), Map("n" -> n3))),
      ("exists(n.name) AND exists(n.surname)", true, false, "", Set(Map("n" -> n1), Map("n" -> n2), Map("n" -> n3)))
    ).foreach {
      case (predicates, useScan, shouldFilter, filterArgument, resultSet) =>
        val query = s"MATCH (n:User) WHERE $predicates RETURN n"

        val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query,
          planComparisonStrategy = ComparePlansWithAssertion(plan => {
            if (shouldFilter)
              plan should includeSomewhere.aPlan("Filter").containingArgumentRegex(filterArgument.r)
            else
              plan shouldNot includeSomewhere.aPlan("Filter")

            if (useScan)
              plan should includeSomewhere.aPlan("NodeIndexScan").containingArgumentForIndexPlan("n", "User", Seq("name", "surname"))
            else
              plan should includeSomewhere.aPlan(s"NodeUniqueIndexSeek").containingArgumentForIndexPlan("n", "User", Seq("name", "surname"), unique = true)
          }))

        result.toComparableResult.toSet should equal(resultSet)
    }
  }

  test("should use composite index for LockingUniqueIndexSeek for nodes in earlier match using indexScan") {
    graph.createNodeKeyConstraint("User", "name", "surname")
    val n1 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Soap"), "User")
    val n2 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Smoke"), "User")
    val n3 = createLabeledNode(Map("name" -> "Jake", "surname" -> "Soap"), "User")

    Seq(
      ("exists(n.name) AND n.surname > 'S'", true, Seq(".*cache\\[n\\.surname\\] > .*".r), Set(Map("n" -> n1, "s" -> n2), Map("n" -> n2, "s" -> n2), Map("n" -> n3, "s" -> n2))),
      ("exists(n.name) AND exists(n.surname)", false, Seq.empty, Set(Map("n" -> n1, "s" -> n2), Map("n" -> n2, "s" -> n2), Map("n" -> n3, "s" -> n2)))
    ).foreach {
      case (predicates, shouldFilter, filterArguments, resultSet) =>
        val query =
          s"""MATCH (n:User), (s:User {name: 'Joe', surname: 'Smoke'})
             |WHERE $predicates
             |MERGE (n)-[:Knows]->(s)
             |RETURN n, s""".stripMargin

        val result = executeWith(Configs.InterpretedAndSlotted, query,
          planComparisonStrategy = ComparePlansWithAssertion(plan => {
            val scanPlan =
              if (shouldFilter)
                aPlan("Filter").containingArgumentRegex(filterArguments: _*)
                  .onTopOf(aPlan("NodeIndexScan").containingArgumentForIndexPlan("n", "User", Seq("name", "surname")))
              else
                aPlan("NodeIndexScan").containingArgumentForIndexPlan("n", "User", Seq("name", "surname"))

            plan should includeSomewhere.aPlan("CartesianProduct")
              .withChildren(
                scanPlan,
                aPlan("NodeUniqueIndexSeek(Locking)")
                  .containingArgumentForIndexPlan("s", "User", Seq("name", "surname"), unique = true))
          }))

        result.toComparableResult.toSet should equal(resultSet)
    }
  }

  test("should use composite index for LockingUniqueIndexSeek for nodes in earlier match with ranges") {
    graph.createNodeKeyConstraint("User", "name", "surname")
    val n1 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Soap"), "User")
    val n2 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Smoke"), "User")
    val n3 = createLabeledNode(Map("name" -> "Jake", "surname" -> "Soap"), "User")

    Seq(
      ("n.name = 'Joe' AND n.surname STARTS WITH 'So'",  false, "", Set(Map("n" -> n1, "s" -> n2))),
      ("n.name >= 'Jo' AND n.surname STARTS WITH 'So'",  true, ".*cache\\[n\\.surname\\] STARTS WITH .*", Set(Map("n" -> n1, "s" -> n2))),
      ("n.name <= 'Je' AND exists(n.surname)",  false, "", Set(Map("n" -> n3, "s" -> n2))),
    ).foreach {
      case (predicates, shouldFilter, filterArgument, resultSet) =>
        val query =
          s"""MATCH (n:User), (s:User {name: 'Joe', surname: 'Smoke'})
             |WHERE $predicates
             |MERGE (n)-[:Knows]->(s)
             |RETURN n, s""".stripMargin

        val result = executeWith(Configs.InterpretedAndSlotted, query,
          planComparisonStrategy = ComparePlansWithAssertion(plan => {
            val lhs =
              if (shouldFilter)
                aPlan("Filter").containingArgumentRegex(filterArgument.r)
                  .onTopOf(aPlan(s"NodeUniqueIndexSeek").containingArgumentForIndexPlan("n", "User", Seq("name", "surname"), unique = true))
              else
                aPlan(s"NodeUniqueIndexSeek").containingArgumentForIndexPlan("n", "User", Seq("name", "surname"), unique = true)

            plan should includeSomewhere.aPlan("CartesianProduct")
              .withChildren(lhs,
                            aPlan("NodeUniqueIndexSeek(Locking)")
                              .containingArgumentForIndexPlan("s", "User", Seq("name", "surname"), unique = true))
          }))

        result.toComparableResult.toSet should equal(resultSet)
    }
  }

  test("should use composite index for LockingUniqueIndexSeek when one bound node with single value") {
    graph.createNodeKeyConstraint("User", "name", "surname")
    val n2 = createLabeledNode(Map("name" -> "Jake", "surname" -> "Soap"), "User")

    val query =
      """PROFILE
         |MATCH (n:User)
         |WHERE n.name = 'Jake' AND n.surname = 'Soap'
         |MERGE (n)-[r:Knows {different: $diff}]->(s:User {name: $name, surname: 'Smoke'})
         |RETURN n, r.different""".stripMargin

    val resultCreate = executeWith(Configs.InterpretedAndSlotted, query, params = Map("name" -> "Joe", "diff" -> 1),
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("Apply")
          .withLHS(aPlan("NodeUniqueIndexSeek(Locking)")
            .containingArgumentForIndexPlan("n", "User", Seq("name", "surname"), unique = true)
            .withRows(1)
            .withExactVariables("n")
          )
          .withRHS(
            includeSomewhere.aPlan("NodeUniqueIndexSeek(Locking)")
              .containingArgumentForIndexPlan("s", "User", Seq("name", "surname"), unique = true)
              .withRows(0)
              .withExactVariables("s", "n")
          )
      }))

    val n1 = createLabeledNode(Map("name" -> "Jake", "surname" -> "Smoke"), "User")
    relate(n2, n1, "Knows", Map("different" -> 2))

    val resultMatch = executeWith(Configs.InterpretedAndSlotted, query, params = Map("name" -> "Jake", "diff" -> 2),
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("Apply")
          .withLHS(aPlan("NodeUniqueIndexSeek(Locking)")
            .containingArgumentForIndexPlan("n", "User", Seq("name", "surname"), unique = true)
            .withRows(1)
            .withExactVariables("n")
          )
          .withRHS(
            includeSomewhere.aPlan("NodeUniqueIndexSeek(Locking)")
              .containingArgumentForIndexPlan("s", "User", Seq("name", "surname"), unique = true)
              .withRows(1)
              .withExactVariables("s", "n")
          )
      }))

    resultCreate.toComparableResult.toSet should equal(Set(Map("n" -> n2, "r.different" -> 1)))
    resultMatch.toComparableResult.toSet should equal(Set(Map("n" -> n2, "r.different" -> 2)))
  }

  test("should use composite index for LockingUniqueIndexSeek when one bound node with more than one value") {
    graph.createNodeKeyConstraint("User", "name", "surname")
    val n1 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Soap"), "User")
    val n2 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Smoke"), "User")
    val n3 = createLabeledNode(Map("name" -> "Jake", "surname" -> "Soap"), "User")
    val r1 = relate(n1, n3, "Knows")
    val r2 = relate(n2, n3, "Knows")

    val query =
      """PROFILE
         |MATCH (n:User)
         |WHERE n.name = 'Joe' AND n.surname >= 'S'
         |MERGE (n)-[r:Knows]->(s:User {name: 'Jake', surname: 'Soap'})
         |RETURN n, r, s""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("Apply")
          .withLHS(aPlan("NodeUniqueIndexSeek")
            .containingArgumentForIndexPlan("n", "User", Seq("name", "surname"), unique = true)
            .withRows(2)
            .withExactVariables("n")
          )
          .withRHS(
            includeSomewhere.aPlan("NodeUniqueIndexSeek(Locking)")
              .containingArgumentForIndexPlan("s", "User", Seq("name", "surname"), unique = true)
              .withRows(2)
              .withExactVariables("s", "n")
          )
      }))

    result.toComparableResult.toSet should equal(Set(Map("n" -> n1, "r" -> r1, "s" -> n3), Map("n" -> n2, "r" -> r2, "s" -> n3)))
  }

  test("should use composite index for LockingUniqueIndexSeek when matching") {
    graph.createNodeKeyConstraint("User", "name", "surname")
    var n = createLabeledNode(Map("name" -> "Joei", "surname" -> "Soap"), "User")
    for (i <- 0 until 10) {
      val x = createLabeledNode(Map("name" -> s"Joe$i", "surname" -> "Soap"), "User")
      relate(n, x, "Knows")
      n = x
    }
    val n1 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Smoke"), "User")
    val n2 = createLabeledNode(Map("name" -> "Jake", "surname" -> "Soap"), "User")
    val r = relate(n2, n1, "Knows")

    val query =
      """
         |MERGE (n:User {name: 'Jake', surname: 'Soap'})-[r:Knows]->(s:User {name: 'Joe', surname: 'Smoke'})
         |RETURN n, r, s""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("CartesianProduct")
          .withChildren(
            aPlan("NodeUniqueIndexSeek(Locking)")
              .containingArgumentForIndexPlan("n", "User", Seq("name", "surname"), unique = true)
              .withExactVariables("n"),
            aPlan("NodeUniqueIndexSeek(Locking)")
              .containingArgumentForIndexPlan("s", "User", Seq("name", "surname"), unique = true)
              .withExactVariables("s")
          )
      }))

    result.toComparableResult.toSet should equal(Set(Map("n" -> n2, "r" -> r, "s" -> n1)))
  }

  test("should use composite index for LockingUniqueIndexSeek when creating") {
    graph.createNodeKeyConstraint("User", "name", "surname")

    val query =
      """PROFILE
         |MERGE (n:User {name: 'Jake', surname: 'Soap'})-[:Knows]->(s:User {name: 'Joe', surname: 'Smoke'})
         |RETURN n.name + ' ' + n.surname AS nname, s.name + ' ' + s.surname AS sname""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        plan should (
          includeSomewhere.aPlan("Either")
            .withRHS(
              aPlan("MergeCreateRelationship")
                .withRows(1)
                .onTopOf(
                  aPlan("MergeCreateNode")
                    .withRows(1)
                    .onTopOf(
                      aPlan("MergeCreateNode")
                        .withRows(1)
                    )
                )
            )
            and (
            includeSomewhere.aPlan("NodeUniqueIndexSeek(Locking)")
              .containingArgumentForIndexPlan("n", "User", Seq("name", "surname"), unique = true, caches = true)
              .withRows(0)
              .withExactVariables("n")
              or
              includeSomewhere.aPlan("NodeUniqueIndexSeek(Locking)")
                .containingArgumentForIndexPlan("s", "User", Seq("name", "surname"), unique = true, caches = true)
                .withRows(0)
                .withExactVariables("s")
            )
          )
      }))

    result.toComparableResult should equal(Seq(Map("sname" -> "Joe Smoke", "nname" -> "Jake Soap")))
  }

  test("should be able to return values from merge for composite index") {
    graph.createNodeKeyConstraint("User", "name", "surname")
    val n1 = createLabeledNode(Map("name" -> "Jake", "surname" -> "Smoke"), "User")
    val n2 = createLabeledNode(Map("name" -> "Jake", "surname" -> "Soap"), "User")
    relate(n2, n1, "Knows")

    val query =
      """PROFILE
        |MATCH (n:User)
        |WHERE n.name = 'Jake' AND n.surname = 'Soap'
        |MERGE (n)-[:Knows]->(s:User {name: 'Jake', surname: 'Smoke'})
        |RETURN s.name + ' ' + s.surname AS name""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("Apply")
          .withLHS(aPlan("NodeUniqueIndexSeek(Locking)")
            .containingArgumentForIndexPlan("n", "User", Seq("name", "surname"), unique = true)
            .withRows(1)
            .withExactVariables("n")
          )
          .withRHS(
            includeSomewhere.aPlan("NodeUniqueIndexSeek(Locking)")
              .containingArgumentForIndexPlan("s", "User", Seq("name", "surname"), unique = true)
              .withRows(1)
              .withExactVariables("s", "n")
          )
      }))

    result.toComparableResult should equal(Seq(Map("name" -> "Jake Smoke")))
  }

  test("should use composite index with combined equality and existence predicates") {
    // Given
    val nodes = setUpMultipleIndexesAndSmallGraph()

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:User) WHERE exists(n.surname) AND n.name = 'Jake' RETURN n",
                             planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek").containingArgumentForIndexPlan("n", "User", Seq("name", "surname"))
        plan should not(includeSomewhere.aPlan("NodeIndexSeek").containingArgumentForIndexPlan("n", "User", Seq("name")))
        plan should not(includeSomewhere.aPlan("NodeIndexSeek").containingArgumentForIndexPlan("s", "User", Seq("surname")))
      }))

    // Then
    result.toComparableResult should equal(List(Map("n" -> nodes.head)))
  }

  test("should use named composite index with combined equality and existence predicates") {
    // Given
    val nodes = setUpMultipleNamedIndexesAndSmallGraph()

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:User) WHERE exists(n.surname) AND n.name = 'Jake' RETURN n",
                             planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek").containingArgumentForIndexPlan("n", "User", Seq("name", "surname"))
        plan should not(includeSomewhere.aPlan("NodeIndexSeek").containingArgumentForIndexPlan("n", "User", Seq("name")))
        plan should not(includeSomewhere.aPlan("NodeIndexSeek").containingArgumentForIndexPlan("n", "User", Seq("surname")))
      }))

    // Then
    result.toComparableResult should equal(List(Map("n" -> nodes.head)))
  }

  test("should use composite index with only existence predicates") {
    // Given
    val nodes = setUpMultipleIndexesAndSmallGraph()

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:User) WHERE exists(n.surname) AND exists(n.name) RETURN n",
                             planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexScan").containingArgumentForIndexPlan("n", "User", Seq("name", "surname"))
        plan should not(includeSomewhere.aPlan("NodeIndexScan").containingArgumentForIndexPlan("n", "User", Seq("name")))
        plan should not(includeSomewhere.aPlan("NodeIndexScan").containingArgumentForIndexPlan("n", "User", Seq("surname")))
      }))

    // Then
    result.toComparableResult.toSet should equal(Set(Map("n" -> nodes(0)), Map("n" -> nodes(1)), Map("n" -> nodes(2))))
  }

  test("should use named composite index with only existence predicates") {
    // Given
    val nodes = setUpMultipleNamedIndexesAndSmallGraph()

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:User) WHERE exists(n.surname) AND exists(n.name) RETURN n",
                             planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexScan").containingArgumentForIndexPlan("n", "User", Seq("name", "surname"))
        plan should not(includeSomewhere.aPlan("NodeIndexScan").containingArgument("n:User(name)"))
        plan should not(includeSomewhere.aPlan("NodeIndexScan").containingArgumentForIndexPlan("n", "User", Seq("surname")))
      }))

    // Then
    result.toComparableResult.toSet should equal(Set(Map("n" -> nodes(0)), Map("n" -> nodes(1)), Map("n" -> nodes(2))))
  }

  test("should be able to update composite index when only one property has changed") {
    graph.createIndex("Person", "name", "surname")
    var n: Node = null
    graph.withTx( tx => {
      n = tx.execute("CREATE (n:Person {name:'Joe', surname:'Soap'}) RETURN n").columnAs("n").next().asInstanceOf[Node]
      tx.execute("MATCH (n:Person) SET n.surname = 'Bloggs'")
    })
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:Person) where n.name = 'Joe' and n.surname = 'Bloggs' RETURN n",
                             planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek")
      }))
    result.toComparableResult should equal(List(Map("n" -> n)))
  }

  test("should plan a composite index seek for a multiple property predicate expression when index is created after data") {
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "WITH RANGE(0,10) AS num CREATE (:Person {id:num})") // ensure label cardinality favors index
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "CREATE (n:Person {name:'Joe', surname:'Soap'})")
    graph.createIndex("Person", "name")
    graph.createIndex("Person", "name", "surname")
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:Person) WHERE n.name = 'Joe' AND n.surname = 'Soap' RETURN n",
                planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek").containingArgumentForIndexPlan("n", "Person", Seq("name", "surname"))
      }))
  }

  test("should use composite index correctly given two IN predicates") {
    // Given
    graph.createIndex("Foo", "bar", "baz")

    (0 to 9) foreach { bar =>
      (0 to 9) foreach { baz =>
        createLabeledNode(Map("bar" -> bar, "baz" -> baz, "idx" -> (bar * 10 + baz)), "Foo")
      }
    }

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """MATCH (n:Foo)
        |WHERE n.bar IN [0,1,2,3,4,5,6,7,8,9]
        |  AND n.baz IN [0,1,2,3,4,5,6,7,8,9]
        |RETURN n.idx as x
        |ORDER BY x""".stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek").containingArgumentForIndexPlan("n", "Foo", Seq("bar", "baz"))
      }))

    // Then
    result.toComparableResult should equal((0 to 99).map(i => Map("x" -> i)).toList)
  }

  test("should use composite index correctly with a single exact together with a List of seeks") {
    // Given
    graph.createIndex("Foo", "bar", "baz")

    (0 to 9) foreach { baz =>
      createLabeledNode(Map("bar" -> 1, "baz" -> baz), "Foo")
    }

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """MATCH (n:Foo)
        |WHERE n.bar = 1
        |  AND n.baz IN [0,1,2,3,4,5,6,7,8,9]
        |RETURN n.baz as x
        |ORDER BY x""".stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek").containingArgumentForIndexPlan("n", "Foo", Seq("bar", "baz"))
      }))

    // Then
    result.toComparableResult should equal((0 to 9).map(i => Map("x" -> i)).toList)
  }

  test("should use composite index correctly with a single exact together with a List of seeks, with the exact seek not being first") {
    // Given
    graph.createIndex("Foo", "bar", "baz")

    (0 to 9) foreach { baz =>
      createLabeledNode(Map("baz" -> (baz % 2), "bar" -> baz), "Foo")
    }

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """MATCH (n:Foo)
        |WHERE n.baz = 1
        |  AND n.bar IN [0,1,2,3,4,5,6,7,8,9]
        |RETURN n.bar as x
        |ORDER BY x""".stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek").containingArgumentForIndexPlan("n", "Foo", Seq("bar", "baz"))
      }))

    // Then
    result.toComparableResult should equal(List(Map("x" -> 1), Map("x" -> 3), Map("x" -> 5), Map("x" -> 7), Map("x" -> 9)))
  }

  test("should handle missing properties when populating index") {
    // Given
    createLabeledNode(Map("foo" -> 42), "PROFILES")
    createLabeledNode(Map("foo" -> 42, "bar" -> 1337, "baz" -> 1980), "L")

    // When
    graph.createIndex("L", "foo", "bar", "baz")

    // Then
    graph should haveIndexes(":L(foo,bar,baz)")
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:L {foo: 42, bar: 1337, baz: 1980}) RETURN count(n)",
                             planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek").containingArgumentForIndexPlan("n", "L", Seq("foo", "bar", "baz"))
      }))
    result.toComparableResult should equal(Seq(Map("count(n)" -> 1)))
  }

  test("should handle missing properties when adding to index after creation") {
    // Given
    createLabeledNode(Map("foo" -> 42), "PROFILES")

    // When
    graph.createIndex("L", "foo", "bar", "baz")
    createLabeledNode(Map("foo" -> 42, "bar" -> 1337, "baz" -> 1980), "L")

    // Then
    graph should haveIndexes(":L(foo,bar,baz)")
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:L {foo: 42, bar: 1337, baz: 1980}) RETURN count(n)",
                             planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek").containingArgumentForIndexPlan("n", "L", Seq("foo", "bar", "baz"))
      }))
    result.toComparableResult should equal(Seq(Map("count(n)" -> 1)))
  }

  test("should fail on multiple attempts to create a composite index") {
    // Given
    executeWith(Configs.All, "CREATE INDEX FOR (n:Person) ON (n.name, n.surname)")

    // When
    val exception = the[TestFailedException] thrownBy {
      executeWith(Configs.All, "CREATE INDEX FOR (n:Person) ON (n.name, n.surname)")
    }

    // Then
    val message = exception.getCause.getMessage
    message should startWith("An equivalent index already exists")
    message should include("name='index_4a67150f', type='GENERAL BTREE', schema=(:Person {name, surname}), indexProvider='native-btree-1.0' )'.")
  }

  test("should fail on multiple attempts to create a named composite index") {
    // Given
    executeWith(Configs.All, "CREATE INDEX my_index FOR (n:Person) ON (n.name, n.surname)")

    // When
    val exception = the[TestFailedException] thrownBy {
      executeWith(Configs.All, "CREATE INDEX my_index FOR (n:Person) ON (n.name, n.surname)")
    }

    // Then
    val message = exception.getCause.getMessage
    message should startWith("An equivalent index already exists")
    message should include("name='my_index', type='GENERAL BTREE', schema=(:Person {name, surname}), indexProvider='native-btree-1.0' )'.")
  }

  test("should fail on multiple attempts to create a named composite index with different name") {
    // Given
    executeWith(Configs.All, "CREATE INDEX my_index FOR (n:Person) ON (n.name, n.surname)")

    // When
    val exception = the[TestFailedException] thrownBy {
      executeWith(Configs.All, "CREATE INDEX your_index FOR (n:Person) ON (n.name, n.surname)")
    }

    // Then (gets wrapped to TestFailedException)
    exception.getCause should have message "There already exists an index (:Person {name, surname})."
  }

  test("should fail on multiple attempts to create a named composite index with different schema") {
    // Given
    executeWith(Configs.All, "CREATE INDEX my_index FOR (n:Person) ON (n.name, n.surname)")

    // When
    val exception = the[TestFailedException] thrownBy {
      executeWith(Configs.All, "CREATE INDEX my_index FOR (n:Person) ON (n.name, n.age)")
    }

    // Then (gets wrapped to TestFailedException)
    exception.getCause should have message "There already exists an index called 'my_index'."
  }

  test("should use range queries against a composite index") {
    // Given
    graph.createIndex("X", "p1", "p2")
    val n = createLabeledNode(Map("p1" -> 1, "p2" -> 1), "X")

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "match (n:X) where n.p1 = 1 AND n.p2 > 0 return n;",
                             planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek").containingArgumentForIndexPlan("n", "X", Seq("p1", "p2"))
      }))

    // Then
    result.toComparableResult should equal(Seq(Map("n" -> n)))
  }

  test("should use range queries against a named composite index") {
    // Given
    graph.createIndexWithName("x_index", "X", "p1", "p2")
    val n = createLabeledNode(Map("p1" -> 1, "p2" -> 1), "X")

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "match (n:X) where n.p1 = 1 AND n.p2 > 0 return n;",
                             planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek").containingArgumentForIndexPlan("n", "X", Seq("p1", "p2"))
      }))

    // Then
    result.toComparableResult should equal(Seq(Map("n" -> n)))
  }

  test("should use composite index for index scan") {
    // Given
    graph.createIndex("X", "p1", "p2")
    createLabeledNode(Map("p1" -> 1, "p2" -> 2), "X")
    createLabeledNode(Map("p1" -> 1, "p2" -> 1), "X")
    createLabeledNode(Map("p1" -> 1, "p2" -> 3), "X")

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n:X) WHERE exists(n.p1) AND n.p2 > 1 RETURN n.p1 AS p1, n.p2 AS p2",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexScan").containingArgumentForIndexPlan("n", "X", Seq("p1", "p2"))
      }))

    // Then
    result.toComparableResult should equal(Seq(Map("p1" -> 1, "p2" -> 2), Map("p1" -> 1, "p2" -> 3)))
  }

  test("should use named composite index for index scan") {
    // Given
    graph.createIndexWithName("x_index", "X", "p1", "p2")
    createLabeledNode(Map("p1" -> 1, "p2" -> 2), "X")
    createLabeledNode(Map("p1" -> 1, "p2" -> 1), "X")
    createLabeledNode(Map("p1" -> 1, "p2" -> 3), "X")

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n:X) WHERE exists(n.p1) AND n.p2 > 1 RETURN n.p1 AS p1, n.p2 AS p2",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexScan").containingArgumentForIndexPlan("n", "X", Seq("p1", "p2"))
      }))

    // Then
    result.toComparableResult should equal(Seq(Map("p1" -> 1, "p2" -> 2), Map("p1" -> 1, "p2" -> 3)))
  }

  test("should use composite index for ranges") {
    // Given
    graph.createIndex("User", "name", "age")
    graph.createIndex("User", "name", "active")
    graph.createIndex("User", "age", "name")
    graph.createIndex("User", "age", "active")
    graph.createIndex("User", "active", "age")
    val n1 = createLabeledNode(Map("name" -> "Joe", "age" -> 25, "active" -> true), "User")
    val n2 = createLabeledNode(Map("name" -> "Bob", "age" -> 60, "active" -> false), "User")
    val n3 = createLabeledNode(Map("name" -> "Alice", "age" -> 6, "active" -> false), "User")
    val n4 = createLabeledNode(Map("name" -> "Joe", "age" -> 45, "active" -> false), "User")
    val n5 = createLabeledNode(Map("name" -> "Jake", "age" -> 16, "active" -> true), "User")
    for (i <- 1 to 100) {
      createLabeledNode(Map("name" -> s"Charlie$i"), "User")
      createLabeledNode(Map("age" -> i), "User")
      createLabeledNode(Map("active" -> (i % 2 == 0)), "User")
      createLabeledNode("User")
    }

    // For all combinations
    Seq(
      ("n.name STARTS WITH 'B' AND exists(n.age)", "n:User\\(name, age\\).*".r, Set(Map("n" -> n2)), false, ""), // prefix
      ("n.name <= 'C' AND exists(n.active)", "n:User\\(name, active\\).*".r, Set(Map("n" -> n2), Map("n" -> n3)), false, ""), // less than
      ("n.age >= 18 AND exists(n.name)", "n:User\\(age, name\\).*".r, Set(Map("n" -> n1), Map("n" -> n2), Map("n" -> n4)), false, ""), // greater than
      ("n.name STARTS WITH 'B' AND n.age = 19", "n:User\\(age, name\\).*".r, Set.empty, false, ""), // prefix after equality
      ("n.age > 18 AND n.age < 60 AND exists(n.active)", "n:User\\(age, active\\).*".r, Set(Map("n" -> n1), Map("n" -> n4)), false, ""), // range between
      ("n.name = 'Jake' AND n.active > false", "n:User\\(name, active\\).*".r, Set(Map("n" -> n5)), false, ""), // greater than on boolean
      ("n.active < true AND exists(n.age)", "n:User\\(active, age\\).*".r, Set(Map("n" -> n2), Map("n" -> n3), Map("n" -> n4)), false, ""), // less than on boolean
      ("n.active < false AND exists(n.age)", "n:User\\(active, age\\).*".r, Set.empty, false, ""), // less than false
      ("n.active >= false AND n.active <= true AND n.age < 10", "n:User\\(active, age\\).*".r, Set(Map("n" -> n3)), true, ".*cache\\[n\\.age\\] < .*") // range between on boolean
    ).foreach {
      case (predicates, indexOn, resultSet, shouldFilter, filterArgument) =>
        // When
        val query = s"MATCH (n:User) WHERE $predicates RETURN n"
        val config = if (filterArgument.isEmpty) Configs.InterpretedAndSlottedAndPipelined else Configs.InterpretedAndSlottedAndPipelined
        val result = executeWith(config, query,
                                 planComparisonStrategy = ComparePlansWithAssertion(plan => {
            if (shouldFilter)
              plan should includeSomewhere.aPlan("Filter").containingArgumentRegex(filterArgument.r)
            else
              plan shouldNot includeSomewhere.aPlan("Filter")

            plan should includeSomewhere.aPlan(s"NodeIndexSeek").containingArgumentRegex(indexOn)
          }))

        // Then
        result.toComparableResult.toSet should equal(resultSet)
    }
  }

  test("should use composite index for range, prefix, suffix, contains and exists predicates") {
    // Given
    graph.createIndex("User", "name", "surname", "age", "active")
    val n = createLabeledNode(Map("name" -> "joe", "surname" -> "soap", "age" -> 25, "active" -> true), "User")

    // For all combinations
    Seq(
      ("n.name = 'joe' AND n.surname = 'soap' AND n.age = 25 AND n.active = true", false, false, Seq.empty), // all equality
      ("n.surname = 'soap' AND n.age = 25 AND n.active = true AND n.name = 'joe'", false, false, Seq.empty), // different order
      ("n.name = 'joe' AND n.surname = 'soap' AND n.age = 25 AND exists(n.active)", false, false, Seq.empty), // exists()
      ("n.name = 'joe' AND n.surname = 'soap' AND n.age >= 25 AND n.active = true", false, true, Seq(".*cache\\[n\\.active\\] = true.*".r)), // inequality
      ("n.name = 'joe' AND n.surname STARTS WITH 's' AND n.age = 25 AND n.active = true", false, true, Seq(".*cache\\[n\\.active\\] = true.*".r, ".*cache\\[n\\.age\\] = .*".r)), // prefix
      ("n.name = 'joe' AND n.surname ENDS WITH 'p' AND n.age = 25 AND n.active = true", false, true, Seq(".*cache\\[n\\.surname\\] ENDS WITH .*".r, ".*cache\\[n\\.active\\] = true.*".r, ".*cache\\[n\\.age\\] = .*".r)), // suffix
      ("n.name >= 'i' AND n.surname = 'soap' AND n.age = 25 AND n.active = true", false, true, Seq(".*cache\\[n\\.active\\] = true.*".r, ".*cache\\[n\\.surname\\] = .*".r, ".*cache\\[n\\.age\\] = .*".r)), // inequality first
      ("n.name STARTS WITH 'j' AND n.surname = 'soap' AND n.age = 25 AND n.active = true", false, true, Seq(".*cache\\[n\\.active\\] = true.*".r, ".*cache\\[n\\.surname\\] = .*".r, ".*cache\\[n\\.age\\] = .*".r)), // prefix first
      ("n.name CONTAINS 'j' AND n.surname = 'soap' AND n.age = 25 AND n.active = true", true, true, Seq(".*cache\\[n\\.name\\] CONTAINS .*".r, ".*cache\\[n\\.active\\] = true.*".r, ".*cache\\[n\\.surname\\] = .*".r, ".*cache\\[n\\.age\\] = .*".r)), // contains first
      ("n.name = 'joe' AND n.surname STARTS WITH 'soap' AND n.age <= 25 AND exists(n.active)", false, true, Seq(".*cache\\[n\\.age\\] <= .*".r)), // combination: equality, prefix, inequality, exists()
      ("n.name >= 'joe' AND n.surname ENDS WITH 'p' AND n.age = 25 AND n.active = true", false, true, Seq(".*cache\\[n\\.surname\\] ENDS WITH .*".r, ".*cache\\[n\\.active\\] = true.*".r, ".*cache\\[n\\.age\\] = .*".r)), // combination: inequality, suffix, equality, equality
      ("exists(n.name) AND n.surname CONTAINS 'o' AND n.age = 25 AND n.active = true", true, true, Seq(".*cache\\[n\\.surname\\] CONTAINS .*".r, ".*cache\\[n\\.active\\] = true.*".r, ".*cache\\[n\\.age\\] = .*".r)), // combination: inequality, suffix, equality, equality

      ("n.name = 'joe' AND n.surname = 'soap' AND n.age = 25 AND n.active >= true", false, false, Seq.empty), // inequality last
      ("n.name = 'joe' AND n.surname = 'soap' AND n.age >= 25 AND n.active >= true", false, true, Seq(".*cache\\[n\\.active\\] >= true.*".r)), // inequality last two
      ("n.name = 'joe' AND n.surname = 'soap' AND n.age >= 25 AND exists(n.active)", false, false, Seq.empty), // combination: equality, equality, inequality, exists()
      ("n.name = 'joe' AND n.surname >= 'r' AND exists(n.age) AND exists(n.active)", false, false, Seq.empty), // combination: equality, inequality, exists(), exists()
      ("n.name = 'joe' AND exists(n.surname) AND exists(n.age) AND n.active = true", false, true, Seq(".*cache\\[n\\.active\\] = true.*".r)), // combination: equality, exists(), exists(), equality
      ("n.name = 'joe' AND exists(n.surname) AND exists(n.age) AND exists(n.active)", false, false, Seq.empty), // combination: equality, exists(), exists(), exists()
      ("n.name >= 'i' AND exists(n.surname) AND n.age >= 25 AND exists(n.active)", false, true, Seq(".*cache\\[n\\.age\\] >= .*".r)), // combination: inequality, exists(), inequality, exists()
      ("n.name >= 'i' AND exists(n.surname) AND exists(n.age) AND exists(n.active)", false, false, Seq.empty), // combination: inequality, exists(), exists(), exists()
      ("exists(n.name) AND n.surname = 'soap' AND n.age >= 25 AND exists(n.active)", true, true, Seq(".*cache\\[n\\.age\\] >= .*".r, ".*cache\\[n\\.surname\\] = .*".r)), // exists() first
      ("exists(n.name) AND exists(n.surname) AND exists(n.age) AND exists(n.active)", true, false, Seq.empty) // all exists
    ).foreach {
      case (predicates, useScan, shouldFilter, filterStrings) =>

        // When
        val query = s"MATCH (n:User) WHERE $predicates RETURN n"
        val config = if (filterStrings.isEmpty) Configs.InterpretedAndSlottedAndPipelined else Configs.InterpretedAndSlottedAndPipelined
        val result = try {
          executeWith(config, query,
            planComparisonStrategy = ComparePlansWithAssertion(plan => {
              //THEN
              if (shouldFilter) {
                plan should includeSomewhere.aPlan("Filter").containingArgumentRegex(filterStrings: _*)
                plan shouldNot includeSomewhere.aPlan("Filter").containingArgumentRegex(".*exists\\(cache\\[n\\..*\\]\\).*".r)
              } else
                plan shouldNot includeSomewhere.aPlan("Filter")

              if (useScan)
                plan should includeSomewhere.aPlan("NodeIndexScan").containingArgumentForIndexPlan("n", "User", Seq("name", "surname", "age", "active"))
              else
                plan should includeSomewhere.aPlan(s"NodeIndexSeek").containingArgumentForIndexPlan("n", "User", Seq("name", "surname", "age", "active"))
            }))
        } catch {
          case e: Exception =>
            System.err.println(query)
            throw e
        }
        try {

          // Then
          result.toComparableResult should equal(Seq(Map("n" -> n)))
        } catch {
          case e: Exception =>
            System.err.println(s"Failed for predicates: $predicates")
            System.err.println(result.executionPlanDescription())
            throw e
        }
    }
  }

  test("should use composite index for multiple range comparisons") {
    // Given
    val query =
      """PROFILE MATCH (person:Person)
        |WHERE 10 < person.highScore < 20 AND exists(person.name)
        |RETURN person.name AS name
        |ORDER BY name""".stripMargin
    val expected = Seq(Map("name" -> "p1"), Map("name" -> "p2"), Map("name" -> "p6"), Map("name" -> "p7"))

    // Nodes in index
    createLabeledNode(Map("name" -> "p1", "highScore" -> 14), "Person")
    createLabeledNode(Map("name" -> "p2", "highScore" -> 16), "Person")
    createLabeledNode(Map("name" -> "p3", "highScore" -> 25), "Person")
    createLabeledNode(Map("name" -> "p4", "highScore" -> 10), "Person")
    createLabeledNode(Map("name" -> "p5", "highScore" -> 3), "Person")
    createLabeledNode(Map("name" -> "p6", "highScore" -> 19), "Person")
    createLabeledNode(Map("name" -> "p7", "highScore" -> 13), "Person")

    // Nodes not in index to ensure index is chosen
    for (i <- 8 to 100) {
      createLabeledNode(Map("name" -> s"p$i"), "Person")
    }

    // Given
    graph.createIndex("Person", "highScore", "name")
    resampleIndexes()

    // When
    val res = executeWith(Configs.InterpretedAndSlottedAndPipelined, query,
                          planComparisonStrategy = ComparePlansWithAssertion(plan => {
        // Then
        plan should includeSomewhere.aPlan(s"NodeIndexSeek")
          .containingArgumentForIndexPlan("person", "Person", Seq("highScore", "name")).withRows(4)
      }))
    // Then
    res.toComparableResult should be(expected)

    // Given
    executeSingle("DROP INDEX ON :Person(highScore,name)")
    graph.createIndex("Person", "name", "highScore")

    // More nodes not in index to ensure index is chosen
    for (i <- 100 to 200) {
      createLabeledNode(Map("name" -> s"p$i"), "Person")
    }

    resampleIndexes()

    // When
    val res2 = executeWith(Configs.InterpretedAndSlottedAndPipelined, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        // Then
        plan should includeSomewhere.aPlan("Filter")
          .containingArgument(
            "cache[person.highScore] > $autoint_0 AND cache[person.highScore] < $autoint_1"
          )
          .withRows(4)
          .onTopOf(aPlan(s"NodeIndexScan")
            .containingArgumentForIndexPlan("person", "Person", Seq("name", "highScore"))
            .withRows(7)
          )
      }))
    // Then
    res2.toComparableResult should be(expected)
  }

  test("should use composite index for is not null and regex") {
    // Given
    graph.createIndex("User", "name", "surname")
    createLabeledNode(Map("name" -> "Joe", "surname" -> "Soap"), "User")
    createLabeledNode(Map("name" -> "Ivan", "surname" -> "Soap"), "User")
    createLabeledNode(Map("name" -> "Ivan", "surname" -> "Smoke"), "User")
    for (i <- 1 to 100) {
      val (name, surname) = if (i % 2 == 0) ("Joe", "Soap") else ("Ivan", "Smoke")
      createLabeledNode(Map("name" -> name), "User")
      createLabeledNode(Map("surname" -> surname), "User")
      createLabeledNode("User")
    }

    // For all combinations
    Seq(
      ("n.name = 'Joe' AND n.surname IS NOT NULL", Set(Map("name" -> "Joe Soap")), false, false, Seq.empty),
      ("n.name = 'Ivan' AND n.surname =~ 'S.*p'", Set(Map("name" -> "Ivan Soap")), false, true, Seq(".*cache\\[n\\.surname\\] =~ .*".r)),
      ("n.name >= 'J' AND n.surname IS NOT NULL", Set(Map("name" -> "Joe Soap")), false, false, Seq.empty),
      ("n.name <= 'J' AND n.surname =~ 'S.*p'", Set(Map("name" -> "Ivan Soap")), false, true, Seq(".*cache\\[n\\.surname\\] =~ .*".r)),
      ("exists(n.name) AND n.surname IS NOT NULL", Set(Map("name" -> "Joe Soap"), Map("name" -> "Ivan Soap"), Map("name" -> "Ivan Smoke")), true, false, Seq.empty),
      ("exists(n.name) AND n.surname =~ 'S.*p'", Set(Map("name" -> "Joe Soap"), Map("name" -> "Ivan Soap")), true, true, Seq(".*cache\\[n\\.surname\\] =~ .*".r)),

      ("n.name IS NOT NULL AND n.surname = 'Smoke'", Set(Map("name" -> "Ivan Smoke")), true, true, Seq(".*cache\\[n\\.surname\\] = .*".r)),
      ("n.name IS NOT NULL AND n.surname =~ 'Sm.*e'", Set(Map("name" -> "Ivan Smoke")), true, true, Seq(".*cache\\[n\\.surname\\] =~ .*".r)),
      ("n.name IS NOT NULL AND n.surname < 'So'", Set(Map("name" -> "Ivan Smoke")), true, true, Seq(".*cache\\[n\\.surname\\] < .*".r)),
      ("n.name IS NOT NULL AND exists(n.surname)", Set(Map("name" -> "Joe Soap"), Map("name" -> "Ivan Soap"), Map("name" -> "Ivan Smoke")), true, false, Seq.empty),
      ("n.name IS NOT NULL AND n.surname IS NOT NULL", Set(Map("name" -> "Joe Soap"), Map("name" -> "Ivan Soap"), Map("name" -> "Ivan Smoke")), true, false, Seq.empty),
      ("n.name =~ 'J.*' AND n.surname = 'Smoke'", Set.empty, true, true, Seq(".*cache\\[n\\.name\\] =~ .*".r, ".*cache\\[n\\.surname\\] = .*".r)),
      ("n.name =~ 'I.*' AND n.surname =~ 'S.*p'", Set(Map("name" -> "Ivan Soap")), true, true, Seq(".*cache\\[n\\.name\\] =~ .*".r, ".*cache\\[n\\.surname\\] =~ .*".r)),
      ("n.name =~ 'I.*' AND n.surname > 'Sn'", Set(Map("name" -> "Ivan Soap")), true, true, Seq(".*cache\\[n\\.name\\] =~ .*".r, ".*cache\\[n\\.surname\\] > .*".r)),
      ("n.name =~ 'J.*' AND exists(n.surname)", Set(Map("name" -> "Joe Soap")), true, true, Seq(".*cache\\[n\\.name\\] =~ .*".r)),
      ("n.name =~ 'J.*' AND n.surname IS NOT NULL", Set(Map("name" -> "Joe Soap")), true, true, Seq(".*cache\\[n\\.name\\] =~ .*".r))
    ).foreach {
      case (predicates, resultSet, useScan, shouldFilter, filterArguments) =>
        // When
        val config = Configs.InterpretedAndSlottedAndPipelined
        val query = s"MATCH (n:User) WHERE $predicates RETURN (n.name + ' ' + n.surname) AS name"
        withClue(query+"\n") {
          val result = executeWith(config, query,
            planComparisonStrategy = ComparePlansWithAssertion(plan => {
              if (shouldFilter)
                plan should includeSomewhere.aPlan("Filter").containingArgumentRegex(filterArguments: _*)
              else
                plan shouldNot includeSomewhere.aPlan("Filter")

              if (useScan)
                plan should includeSomewhere.aPlan("NodeIndexScan").containingArgumentForIndexPlan("n", "User", Seq("name", "surname"))
              else
                plan should includeSomewhere.aPlan(s"NodeIndexSeek").containingArgumentForIndexPlan("n", "User", Seq("name", "surname"))
            }))

          // Then
          result.toComparableResult.toSet should equal(resultSet)
        }
    }
  }

  test("nested index join with composite indexes") {
    // given
    (1 to 1000) foreach { _ => // Get the planner to do what we expect it to!
      createLabeledNode("X")
    }
    val a = createNode("p1" -> 1, "p2" -> 1)
    val b = createLabeledNode(Map("p1" -> 1, "p2" -> 1), "X")
    graph.createIndex("X", "p1", "p2")

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "match (a), (b:X) where id(a) = $id AND b.p1 = a.p1 AND b.p2 = 1 return b",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek").containingArgumentForIndexPlan("b", "X", Seq("p1", "p2"))
      }), params = Map("id" -> a.getId))

    result.toComparableResult should equal(Seq(Map("b" -> b)))
  }

  test("should use composite index with spatial") {
    // Given
    val n1 = createLabeledNode(Map("name" -> "Joe", "city" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.2, 5.6).asObjectCopy()), "User")
    createLabeledNode(Map("name" -> "Joe", "city" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.2, 3.4).asObjectCopy()), "User")
    createLabeledNode(Map("name" -> "Joe", "city" -> "P:2-7203[1.2, 5.6]"), "User")

    // When
    val query = "MATCH (n:User) WHERE n.name = 'Joe' AND n.city = point({x: 1.2, y: 5.6}) RETURN n"

    val resultNoIndex = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    graph.createIndex("User", "name", "city")
    resampleIndexes()

    val resultIndex = executeWith(Configs.InterpretedAndSlottedAndPipelined, query,
                                  planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek")
      }))

    // Then
    resultNoIndex.toComparableResult should equal(List(Map("n" -> n1)))
    resultIndex.toComparableResult should equal(resultNoIndex.toComparableResult)
  }

  test("should use composite index with spatial and range") {
    // Given
    val n1 = createLabeledNode(Map("name" -> "Joe", "age" -> 36, "city" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 180, 5.6).asObjectCopy()), "User")
    createLabeledNode(Map("name" -> "Joe", "age" -> 36, "city" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 180, 5.5).asObjectCopy()), "User")

    // When
    val query =
      """MATCH (n:User)
        |WHERE n.name = 'Joe' AND distance(n.city, point({x: 180, y: 5.58, crs: 'WGS-84'})) <= 5000 AND n.age > 35
        |RETURN n
        |""".stripMargin

    val resultNoIndex = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    graph.createIndex("User", "name", "city", "age")
    resampleIndexes()

    val resultIndex = executeWith(Configs.InterpretedAndSlottedAndPipelined, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("Filter").containingArgumentRegex(".*distance\\(cache\\[n.city\\], point.*".r, ".*cache\\[n\\.age\\] >.*".r)
        plan should includeSomewhere.aPlan("NodeIndexSeek")
      }))

    // Then
    resultNoIndex.toComparableResult should equal(List(Map("n" -> n1)))
    resultIndex.toComparableResult should equal(resultNoIndex.toComparableResult)
  }

  test("should use composite index with temporal") {
    // Given
    val n1 = createLabeledNode(Map("date" -> LocalDate.of(1991, 10, 18), "time" -> LocalTime.of(21, 22, 0)), "Label")
    createLabeledNode(Map("date" -> LocalDate.of(1991, 10, 18), "time" -> OffsetTime.of(21, 22, 0, 0, ZoneOffset.of("+00:00"))), "Label")
    createLabeledNode(Map("date" -> "1991-10-18", "time" -> LocalTime.of(21, 22, 0)), "Label")
    createLabeledNode(Map("date" -> LocalDate.of(1991, 10, 18).toEpochDay, "time" -> LocalTime.of(21, 22, 0)), "Label")

    // When
    val query =
      """
        |MATCH (n:Label)
        |WHERE n.date = date('1991-10-18') AND n.time = localtime('21:22')
        |RETURN n
      """.stripMargin

    val resultNoIndex = executeWith(Configs.UDF, query)

    graph.createIndex("Label", "date", "time")
    resampleIndexes()

    val resultIndex = executeWith(Configs.UDF, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek")
      }))

    // Then
    resultNoIndex.toComparableResult should equal(List(Map("n" -> n1)))
    resultIndex.toComparableResult should equal(resultNoIndex.toComparableResult)
  }

  test("should use composite index with duration") {
    // Given
    val n1 = createLabeledNode(Map("name" -> "Neo", "result" -> DurationValue.duration(0, 0, 1800, 0).asObject()), "Runner")
    createLabeledNode(Map("name" -> "Neo", "result" -> LocalTime.of(0, 30, 0)), "Runner")
    createLabeledNode(Map("name" -> "Neo", "result" -> "PT30M"), "Runner")

    // When
    val query =
      """
        |MATCH (n:Runner)
        |WHERE n.name = 'Neo' AND n.result = duration('PT30M')
        |RETURN n
      """.stripMargin

    val resultNoIndex = executeWith(Configs.UDF, query)

    graph.createIndex("Runner", "name", "result")
    resampleIndexes()

    val resultIndex = executeWith(Configs.UDF, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek")
      }))

    // Then
    resultNoIndex.toComparableResult should equal(List(Map("n" -> n1)))
    resultIndex.toComparableResult should equal(resultNoIndex.toComparableResult)
  }

  test("should use named composite index with duration") {
    // Given
    val n1 = createLabeledNode(Map("name" -> "Neo", "result" -> DurationValue.duration(0, 0, 1800, 0).asObject()), "Runner")
    createLabeledNode(Map("name" -> "Neo", "result" -> LocalTime.of(0, 30, 0)), "Runner")
    createLabeledNode(Map("name" -> "Neo", "result" -> "PT30M"), "Runner")

    // When
    val query =
      """
        |MATCH (n:Runner)
        |WHERE n.name = 'Neo' AND n.result = duration('PT30M')
        |RETURN n
      """.stripMargin

    val resultNoIndex = executeWith(Configs.UDF, query)

    graph.createIndexWithName("runner_index", "Runner", "name", "result")
    resampleIndexes()

    val resultIndex = executeWith(Configs.UDF, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek").containingArgumentForIndexPlan("n", "Runner", Seq("name", "result"))
      }))

    // Then
    resultNoIndex.toComparableResult should equal(List(Map("n" -> n1)))
    resultIndex.toComparableResult should equal(resultNoIndex.toComparableResult)
  }

  // Test when having nodes in txState
  test("should use composite index and get correct value for only equality") {
    graph.createIndex("Awesome", "prop1", "prop2")
    createNodes()

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n:Awesome) WHERE n.prop1 = 40 AND n.prop2 = 3 RETURN n.prop1, n.prop2",
      executeBefore = createNodesInTxState,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek")
          .containingArgumentForIndexPlan("n", "Awesome", Seq("prop1", "prop2"), caches = true)
          .withExactVariables("n")
        plan should not(includeSomewhere.aPlan("Filter"))
      }))

    result.toList should equal(List(Map("n.prop1" -> 40, "n.prop2" -> 3)))
  }

  test("should use composite index and get correct value for equality followed by range") {
    graph.createIndex("Awesome", "prop1", "prop2", "prop5")
    createNodes()

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      s"MATCH (n:Awesome) WHERE n.prop1 = 44 AND n.prop2 = 1 AND n.prop5 > 'e' RETURN n.prop1, n.prop2, n.prop5",
      executeBefore = createNodesInTxState,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek")
          .containingArgumentForIndexPlan("n", "Awesome", Seq("prop1", "prop2", "prop5"), caches = true)
          .withExactVariables("n")
        plan should not(includeSomewhere.aPlan("Filter"))
      }))

    val expected = Set(
      Map[Any, Any]("n.prop1" -> 44, "n.prop2" -> 1, "n.prop5" -> "f"),
      Map[Any, Any]("n.prop1" -> 44, "n.prop2" -> 1, "n.prop5" -> "g")
    )
    result.toSet should equal(expected)
  }

  test("should use composite index and get correct value for equality followed by STARTS WITH") {
    graph.createIndex("Awesome", "prop1", "prop2")
    createNodes()

    def createMe(tx: InternalTransaction): Unit = {
      createNodesInTxState(tx)
      // Values that should not be valid for the query
      tx.execute(
        """
          |CREATE (:Awesome {prop2: 42, prop1: 'futhark'})
          |CREATE (:Awesome {prop2: false, prop1: 'futhark'})
          |CREATE (:Awesome {prop2: ['foo', 'bar'], prop1: 'futhark'})
          |""".stripMargin)
    }

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n:Awesome) WHERE n.prop1 = 'futhark' AND n.prop2 STARTS WITH 'o' RETURN n.prop1, n.prop2",
      executeBefore = createMe,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek")
          .containingArgumentForIndexPlan("n", "Awesome", Seq("prop1", "prop2"), caches = true)
          .withExactVariables("n")
        plan should not(includeSomewhere.aPlan("Filter"))
      }))

    val expected = Set(
      Map("n.prop1" -> "futhark", "n.prop2" -> "otter"),
      Map("n.prop1" -> "futhark", "n.prop2" -> "owl")
    )
    result.toSet should equal(expected)
  }

  test("should use composite index and get correct value for equality followed by exists") {
    graph.createIndex("Awesome", "prop1", "prop2")
    createNodes()

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n:Awesome) WHERE n.prop1 = 40 AND exists(n.prop2) RETURN n.prop1, n.prop2",
      executeBefore = createNodesInTxState,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek")
          .containingArgumentForIndexPlan("n", "Awesome", Seq("prop1", "prop2"), caches = true)
          .withExactVariables("n")
        plan should not(includeSomewhere.aPlan("Filter"))
      }))

    val expected = Set(
      Map("n.prop1" -> 40, "n.prop2" -> 0),
      Map("n.prop1" -> 40, "n.prop2" -> 1),
      Map("n.prop1" -> 40, "n.prop2" -> 3),
      Map("n.prop1" -> 40, "n.prop2" -> 5)
    )
    result.toSet should equal(expected)
  }

  test("should use composite index and get correct value for range followed by equality") {
    graph.createIndex("Awesome", "prop1", "prop2")
    createNodes()

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n:Awesome) WHERE n.prop1 >= 40 AND n.prop2 = 1 RETURN n.prop1, n.prop2",
      executeBefore = createNodesInTxState,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("Filter")
          .containingArgumentRegex(".*cache\\[n\\.prop2\\] = .*".r)
          .onTopOf(aPlan("NodeIndexSeek")
            .containingArgumentForIndexPlan("n", "Awesome", Seq("prop1", "prop2"), caches = true)
            .withExactVariables("n"))
      }))

    val expected = Set(
      Map("n.prop1" -> 40, "n.prop2" -> 1),
      Map("n.prop1" -> 43, "n.prop2" -> 1),
      Map("n.prop1" -> 44, "n.prop2" -> 1),
      Map("n.prop1" -> 44, "n.prop2" -> 1),
      Map("n.prop1" -> 44, "n.prop2" -> 1),
      Map("n.prop1" -> 44, "n.prop2" -> 1)
    )
    result.toSet should equal(expected)
  }

  test("should use composite index and get correct value for only ranges") {
    graph.createIndex("Awesome", "prop1", "prop2")
    createNodes()

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n:Awesome) WHERE n.prop1 > 40 AND n.prop2 > 1 RETURN n.prop1, n.prop2",
      executeBefore = createNodesInTxState,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("Filter")
          .containingArgumentRegex(".*cache\\[n\\.prop2\\] > .*".r)
          .onTopOf(aPlan("NodeIndexSeek")
            .containingArgumentForIndexPlan("n", "Awesome", Seq("prop1", "prop2"), caches = true)
            .withExactVariables("n"))
      }))

    val expected = Set(
      Map("n.prop1" -> 41, "n.prop2" -> 2),
      Map("n.prop1" -> 42, "n.prop2" -> 3),
      Map("n.prop1" -> 44, "n.prop2" -> 4),
      Map("n.prop1" -> 44, "n.prop2" -> 3)
    )
    result.toSet should equal(expected)
  }

  test("should use composite index and get correct value for range followed by exists") {
    graph.createIndex("Awesome", "prop1", "prop2")
    createNodes()

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n:Awesome) WHERE n.prop1 < 44 AND exists(n.prop2) RETURN n.prop1, n.prop2",
      executeBefore = createNodesInTxState,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek")
          .containingArgumentForIndexPlan("n", "Awesome", Seq("prop1", "prop2"), caches = true)
          .withExactVariables("n")
        plan should not(includeSomewhere.aPlan("Filter"))
      }))

    val expected = Set(
      Map("n.prop1" -> 40, "n.prop2" -> 0),
      Map("n.prop1" -> 40, "n.prop2" -> 1),
      Map("n.prop1" -> 40, "n.prop2" -> 3),
      Map("n.prop1" -> 40, "n.prop2" -> 5),
      Map("n.prop1" -> 41, "n.prop2" -> 2),
      Map("n.prop1" -> 42, "n.prop2" -> 3),
      Map("n.prop1" -> 43, "n.prop2" -> 1)
    )
    result.toSet should equal(expected)
  }

  test("should use composite index and get correct value for only exists") {
    graph.createIndex("Awesome", "prop1", "prop2")
    createNodes()

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      s"MATCH (n:Awesome) WHERE exists(n.prop1) AND exists(n.prop2) RETURN n.prop1, n.prop2",
      executeBefore = createNodesInTxState,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexScan")
          .containingArgumentForIndexPlan("n", "Awesome", Seq("prop1", "prop2"), caches = true)
          .withExactVariables("n")
        plan should not(includeSomewhere.aPlan("Filter"))
      }))

    val expected = Set(
      Map("n.prop1" -> 40, "n.prop2" -> 0),
      Map("n.prop1" -> 40, "n.prop2" -> 1),
      Map("n.prop1" -> 40, "n.prop2" -> 3),
      Map("n.prop1" -> 40, "n.prop2" -> 5),
      Map("n.prop1" -> 41, "n.prop2" -> 2),
      Map("n.prop1" -> 42, "n.prop2" -> 3),
      Map("n.prop1" -> 43, "n.prop2" -> 1),
      Map("n.prop1" -> 44, "n.prop2" -> 1),
      Map("n.prop1" -> 44, "n.prop2" -> 1),
      Map("n.prop1" -> 44, "n.prop2" -> 1),
      Map("n.prop1" -> 44, "n.prop2" -> 1),
      Map("n.prop1" -> 44, "n.prop2" -> 3),
      Map("n.prop1" -> 44, "n.prop2" -> 4),

      Map("n.prop1" -> "aismfama", "n.prop2" -> "rab"),
      Map("n.prop1" -> "alpha", "n.prop2" -> "ant"),
      Map("n.prop1" -> "fehu", "n.prop2" -> "whale"),
      Map("n.prop1" -> "foo", "n.prop2" -> "alligator"),
      Map("n.prop1" -> "fooism", "n.prop2" -> "rab"),
      Map("n.prop1" -> "footurama", "n.prop2" -> "bar"),
      Map("n.prop1" -> "futhark", "n.prop2" -> "dragonfly"),
      Map("n.prop1" -> "futhark", "n.prop2" -> "otter"),
      Map("n.prop1" -> "futhark", "n.prop2" -> "owl")
    )
    result.toSet should equal(expected)
  }

  test("should use composite index and get correct value for STARTS WITH") {
    graph.createIndex("Awesome", "prop1", "prop2")
    createNodes()

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n:Awesome) WHERE n.prop1 STARTS WITH 'fo' AND n.prop2 >= '' RETURN n.prop1, n.prop2",
      executeBefore = createNodesInTxState,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("Filter")
          .containingArgumentRegex(".*cache\\[n\\.prop2\\] >= .*".r)
          .onTopOf(aPlan("NodeIndexSeek")
            .containingArgumentForIndexPlan("n", "Awesome", Seq("prop1", "prop2"), caches = true)
            .withExactVariables("n"))
      }))

    val expected = Set(
      Map("n.prop1" -> "foo", "n.prop2" -> "alligator"),
      Map("n.prop1" -> "fooism", "n.prop2" -> "rab"),
      Map("n.prop1" -> "footurama", "n.prop2" -> "bar")
    )
    result.toSet should equal(expected)
  }

  test("should use composite index and get correct value for STARTS WITH when all string values match") {
    graph.createIndex("Awesome", "prop1", "prop2")

    def createMe(tx: InternalTransaction): Unit = {
      tx.execute(
        """
          |CREATE (:Awesome {prop1: 'foo', prop2: 'alligator'})
          |CREATE (:Awesome {prop1: 'futhark', prop2: 'owl'})
          |CREATE (:Awesome {prop1: 'futhark', prop2: 'otter'})
          |CREATE (:Awesome {prop1: 'futhark', prop2: 'dragonfly'})
          |CREATE (:Awesome {prop1: 'fehu', prop2: 'whale'})
        """.stripMargin)

      // Values that should not be valid for the query
      tx.execute(
        """
          |CREATE (:Awesome {prop1: 5, prop2: 'seagull'})
          |CREATE (:Awesome {prop1: false, prop2: 'fish'})
          |CREATE (:Awesome {prop1: true, prop2: 'horse'})
          |CREATE (:Awesome {prop1: 3.14, prop2: 'salmon'})
        """.stripMargin)
    }
    createLabeledNode(Map("prop1" -> LocalDate.of(1991, 10, 18).toEpochDay, "prop2" -> "peacock"), "Awesome")
    createLabeledNode(Map("prop1" -> LocalTime.of(0, 30, 0), "prop2" -> "mole"), "Awesome")
    createLabeledNode(Map("prop1" -> DurationValue.duration(0, 0, 1800, 0).asObject(), "prop2" -> "kangaroo"), "Awesome")

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n:Awesome) WHERE n.prop1 STARTS WITH 'f' AND n.prop2 IS NOT NULL RETURN n.prop1, n.prop2",
      executeBefore = createMe,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek")
          .containingArgumentForIndexPlan("n", "Awesome", Seq("prop1", "prop2"), caches = true)
          .withExactVariables("n")
      }))

    val expected = Set(
      Map("n.prop1" -> "fehu", "n.prop2" -> "whale"),
      Map("n.prop1" -> "foo", "n.prop2" -> "alligator"),
      Map("n.prop1" -> "futhark", "n.prop2" -> "dragonfly"),
      Map("n.prop1" -> "futhark", "n.prop2" -> "otter"),
      Map("n.prop1" -> "futhark", "n.prop2" -> "owl")
    )
    result.toSet should equal(expected)
  }

  test("should use composite index for ENDS WITH") {
    graph.createIndex("Awesome", "prop1", "prop2")
    createNodes()

    // Add nodes not in index
    graph.withTx( tx =>
      tx.execute(
        """
          |CREATE (:Awesome)
          |CREATE (:Awesome)
          |CREATE (:Awesome)
          |CREATE (:Awesome)
          |CREATE (:Awesome)
          |
          |CREATE (:NotAwesome {prop1: 'footurama', prop2: 'bar'})
          |CREATE (:NotAwesome {prop1: 'fooism', prop2: 'rab'})
          |CREATE (:NotAwesome {prop1: 'aismfama', prop2: 'rab'})
      """.stripMargin
      ) )
    resampleIndexes()

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      s"MATCH (n:Awesome) WHERE n.prop1 ENDS WITH 'a' AND n.prop2 >= '' RETURN n.prop1, n.prop2",
      executeBefore = createNodesInTxState,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("Filter")
          .containingArgumentRegex(".*cache\\[n\\.prop1\\] ENDS WITH .*".r, ".*cache\\[n\\.prop2\\] >= .*".r)
          .onTopOf(aPlan("NodeIndexScan").containingArgumentForIndexPlan("n", "Awesome", Seq("prop1", "prop2")))
      }))

    val expected = Set(
      Map("n.prop1" -> "aismfama", "n.prop2" -> "rab"),
      Map("n.prop1" -> "alpha", "n.prop2" -> "ant"),
      Map("n.prop1" -> "footurama", "n.prop2" -> "bar")
    )
    result.toSet should equal(expected)
  }

  test("should use named composite index for ENDS WITH") {
    graph.createIndexWithName("ends_with_index", "Awesome", "prop1", "prop2")
    createNodes()

    // Add nodes not in index
    graph.withTx( tx =>
      tx.execute(
        """
          |CREATE (:Awesome)
          |CREATE (:Awesome)
          |CREATE (:Awesome)
          |CREATE (:Awesome)
          |CREATE (:Awesome)
          |
          |CREATE (:NotAwesome {prop1: 'footurama', prop2: 'bar'})
          |CREATE (:NotAwesome {prop1: 'fooism', prop2: 'rab'})
          |CREATE (:NotAwesome {prop1: 'aismfama', prop2: 'rab'})
      """.stripMargin
      ) )
    resampleIndexes()

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      s"MATCH (n:Awesome) WHERE n.prop1 ENDS WITH 'a' AND n.prop2 >= '' RETURN n.prop1, n.prop2",
      executeBefore = createNodesInTxState,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("Filter")
          .containingArgumentRegex(".*cache\\[n\\.prop1\\] ENDS WITH .*".r, ".*cache\\[n\\.prop2\\] >= .*".r)
          .onTopOf(aPlan("NodeIndexScan").containingArgumentForIndexPlan("n", "Awesome", Seq("prop1", "prop2")))
      }))

    val expected = Set(
      Map("n.prop1" -> "aismfama", "n.prop2" -> "rab"),
      Map("n.prop1" -> "alpha", "n.prop2" -> "ant"),
      Map("n.prop1" -> "footurama", "n.prop2" -> "bar")
    )
    result.toSet should equal(expected)
  }

  test("should use composite index for CONTAINS") {
    graph.createIndex("Awesome", "prop1", "prop2")
    createNodes()

    // Add nodes not in index
    graph.withTx( tx =>
      tx.execute(
        """
          |CREATE (:Awesome)
          |CREATE (:Awesome)
          |CREATE (:Awesome)
          |CREATE (:Awesome)
          |CREATE (:Awesome)
          |
          |CREATE (:NotAwesome {prop1: 'footurama', prop2: 'bar'})
          |CREATE (:NotAwesome {prop1: 'fooism', prop2: 'rab'})
          |CREATE (:NotAwesome {prop1: 'aismfama', prop2: 'rab'})
      """.stripMargin
      ) )
    resampleIndexes()

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      s"MATCH (n:Awesome) WHERE n.prop1 CONTAINS 'foo' AND n.prop2 >= '' RETURN n.prop1, n.prop2",
      executeBefore = createNodesInTxState,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("Filter")
          .containingArgumentRegex(".*cache\\[n\\.prop1\\] CONTAINS .*".r, ".*cache\\[n\\.prop2\\] >= .*".r)
          .onTopOf(aPlan("NodeIndexScan").containingArgumentForIndexPlan("n", "Awesome", Seq("prop1", "prop2")))
      }))

    val expected = Set(
      Map("n.prop1" -> "foo", "n.prop2" -> "alligator"),
      Map("n.prop1" -> "fooism", "n.prop2" -> "rab"),
      Map("n.prop1" -> "footurama", "n.prop2" -> "bar")
    )
    result.toSet should equal(expected)
  }

  test("should use composite index and get correct value for range on spatial - first property") {
    // Given
    graph.createIndex("User", "city", "name")
    val point1 = Values.pointValue(CoordinateReferenceSystem.WGS84, 180, 5.6).asObjectCopy()
    val point2 = Values.pointValue(CoordinateReferenceSystem.WGS84, 180, 4.5).asObjectCopy()

    createLabeledNode(Map("name" -> "Joe", "city" -> point1), "User")
    createLabeledNode(Map("name" -> "Jake", "city" -> point2), "User")

    createLabeledNode(Map("name" -> "Jake", "city" -> point1), "User")
    createLabeledNode(Map("name" -> "Joe", "city" -> point2), "User")

    // Values that should not be valid for the query
    createLabeledNode(Map("name" -> "Joe", "city" -> "Staffanstorp"), "User")
    createLabeledNode(Map("name" -> "Joe", "city" -> false), "User")
    createLabeledNode(Map("name" -> "Joe", "city" -> LocalDate.of(1991, 10, 18).toEpochDay), "User")
    createLabeledNode(Map("name" -> "Joe", "city" -> LocalTime.of(0, 30, 0)), "User")
    createLabeledNode(Map("name" -> "Joe", "city" -> DurationValue.duration(0, 0, 1800, 0).asObject()), "User")

    // When
    val query =
      """MATCH (n:User)
        |WHERE n.name IS NOT NULL AND distance(n.city, point({x: 180, y: 5.58, crs: 'WGS-84'})) <= 5000
        |RETURN n.name, n.city
        |""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek")
          .containingArgumentForIndexPlan("n", "User", Seq("city", "name"), caches = true)
          .withExactVariables("n")
      }))

    // Then
    val expected = Set(
      Map("n.name" -> "Jake", "n.city" -> point1),
      Map("n.name" -> "Joe", "n.city" -> point1)
    )
    result.toSet should equal(expected)
  }

  test("should use composite index and get correct value for range on spatial - later property") {
    // Given
    graph.createIndex("User", "name", "city")
    val point1 = Values.pointValue(CoordinateReferenceSystem.WGS84, 180, 5.6).asObjectCopy()
    val point2 = Values.pointValue(CoordinateReferenceSystem.WGS84, 180, 4.5).asObjectCopy()
    val point3 = Values.pointValue(CoordinateReferenceSystem.WGS84, 180, 5.55).asObjectCopy()

    createLabeledNode(Map("name" -> "Joe", "city" -> point1), "User")
    createLabeledNode(Map("name" -> "Jake", "city" -> point2), "User")

    createLabeledNode(Map("name" -> "Jake", "city" -> point1), "User")
    createLabeledNode(Map("name" -> "Joe", "city" -> point2), "User")
    createLabeledNode(Map("name" -> "Joe", "city" -> point3), "User")

    // Values that should not be valid for the query
    createLabeledNode(Map("name" -> "Joe", "city" -> "Staffanstorp"), "User")
    createLabeledNode(Map("name" -> "Joe", "city" -> false), "User")
    createLabeledNode(Map("name" -> "Joe", "city" -> LocalDate.of(1991, 10, 18).toEpochDay), "User")
    createLabeledNode(Map("name" -> "Joe", "city" -> LocalTime.of(0, 30, 0)), "User")
    createLabeledNode(Map("name" -> "Joe", "city" -> DurationValue.duration(0, 0, 1800, 0).asObject()), "User")

    // When
    val query =
      """MATCH (n:User)
        |WHERE n.name = 'Joe' AND distance(n.city, point({x: 180, y: 5.58, crs: 'WGS-84'})) <= 5000
        |RETURN n.name, n.city
        |""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek")
          .containingArgumentForIndexPlan("n", "User", Seq("name", "city"), caches = true)
          .withExactVariables("n")
      }))

    // Then
    val expected = Set(
      Map("n.name" -> "Joe", "n.city" -> point1),
      Map("n.name" -> "Joe", "n.city" -> point3)
    )
    result.toSet should equal(expected)
  }

  test("should use composite index and get correct answers when not returning values for range") {
    graph.createIndex("Awesome", "prop1", "prop2")
    createNodes()

    var nodes = Set.empty[Node]

    def createMe(tx: InternalTransaction): Unit = {
      createNodesInTxState(tx)
    }
    nodes = Set(
      createLabeledNode(Map("prop1" -> 45, "prop2" -> "hello"), "Awesome"),
      createLabeledNode(Map("prop1" -> 45, "prop2" -> "foo"), "Awesome")
    )

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n:Awesome) WHERE n.prop1 > 44 AND n.prop2 IS NOT NULL RETURN COUNT(n) as c",
      executeBefore = createMe,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek")
          .containingArgumentForIndexPlan("n", "Awesome", Seq("prop1", "prop2"))
          .withExactVariables("n")
        plan should not(includeSomewhere.aPlan("Filter"))
      }))

    result.toSet should equal(Set(Map("c" -> nodes.size)))
  }

  test("should use composite index and get correct answers when not returning values for equality") {
    graph.createIndex("Awesome", "prop1", "prop2")
    createNodes()

    var nodes = Set.empty[Node]

    def createMe(tx:InternalTransaction): Unit = {
      createNodesInTxState(tx)
    }
    nodes = Set(
      createLabeledNode(Map("prop1" -> 45, "prop2" -> "hello"), "Awesome"),
      createLabeledNode(Map("prop1" -> 45, "prop2" -> "foo"), "Awesome")
    )

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n:Awesome) WHERE n.prop1 = 45 AND n.prop2 STARTS WITH 'h' RETURN COUNT(n) as c",
      executeBefore = createMe,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek")
          .containingArgumentForIndexPlan("n", "Awesome", Seq("prop1", "prop2"))
          .withExactVariables("n")
        plan should not(includeSomewhere.aPlan("Filter"))
      }))

    result.toSet should equal(Set(Map("c" -> 1)))
  }

  case class haveIndexes(expectedIndexes: String*) extends Matcher[GraphDatabaseQueryService] {
    def apply(graph: GraphDatabaseQueryService): MatchResult = {
      graph.withTx( tx => {
        val indexNames = tx.schema().getIndexes.asScala.toList.map(
          i => s":${i.getLabels.asScala.toList.mkString(",")}(${i.getPropertyKeys.asScala.toList.mkString(",")})")
        val result = expectedIndexes.forall(i => indexNames.contains(i.toString))
        MatchResult(
          result,
          s"Expected graph to have indexes ${expectedIndexes.mkString(", ")}, but it was ${indexNames.mkString(", ")}",
          s"Expected graph to not have indexes ${expectedIndexes.mkString(", ")}, but it did."
        )
      } )
    }
  }

  private def createNodes(): Unit =
    graph.withTx( tx =>
      tx.execute(
        """
          |CREATE (:Awesome {prop1: 40, prop2: 5})
          |CREATE (:Awesome {prop1: 41, prop2: 2})
          |CREATE (:Awesome {prop1: 42, prop2: 3})
          |CREATE (:Awesome {prop1: 43, prop2: 1})
          |CREATE (:Awesome {prop1: 44, prop2: 3})
          |
          |CREATE (:Awesome {prop1: 'footurama', prop2: 'bar'})
          |CREATE (:Awesome {prop1: 'fooism', prop2: 'rab'})
          |CREATE (:Awesome {prop1: 'aismfama', prop2: 'rab'})
      """.stripMargin )
    )

  private def createNodesInTxState(tx: InternalTransaction): Unit =
    tx.execute(
      """
        |CREATE (:Awesome {prop1: 40, prop2: 3, prop5: 'a'})
        |CREATE (:Awesome {prop1: 40, prop2: 1, prop5: 'b'})
        |CREATE (:Awesome {prop1: 40, prop2: 0, prop5: 'c'})
        |CREATE (:Awesome {prop1: 44, prop2: 4, prop5: 'd'})
        |CREATE (:Awesome {prop1: 44, prop2: 1, prop5: 'e'})
        |CREATE (:Awesome {prop1: 44, prop2: 1, prop5: 'f'})
        |CREATE (:Awesome {prop1: 44, prop2: 1, prop5: 'g'})
        |CREATE (:Awesome {prop1: 44, prop2: 1, prop5: 0})
        |
        |CREATE (:Awesome {prop1: 'alpha', prop2: 'ant'})
        |CREATE (:Awesome {prop1: 'foo', prop2: 'alligator'})
        |CREATE (:Awesome {prop1: 'futhark', prop2: 'owl'})
        |CREATE (:Awesome {prop1: 'futhark', prop2: 'otter'})
        |CREATE (:Awesome {prop1: 'futhark', prop2: 'dragonfly'})
        |CREATE (:Awesome {prop1: 'fehu', prop2: 'whale'})
      """.stripMargin
    )

  private def setUpMultipleIndexesAndSmallGraph() = {
    graph.createIndex("User", "name")
    graph.createIndex("User", "surname")
    graph.createIndex("User", "name", "surname")
    val n0 = createLabeledNode(Map("name" -> "Jake", "surname" -> "Soap"), "User")
    val n1 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Soap"), "User")
    val n2 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Smoke"), "User")
    for (_ <- 1 to 100) {
      createLabeledNode(Map("name" -> "Jake"), "User")
      createLabeledNode(Map("surname" -> "Soap"), "User")
      createLabeledNode("User")
    }
    resampleIndexes()
    List(n0, n1, n2)
  }

  private def setUpMultipleNamedIndexesAndSmallGraph() = {
    graph.createIndexWithName("name_index", "User", "name")
    graph.createIndexWithName("surname_index", "User", "surname")
    graph.createIndexWithName("composite_index", "User", "name", "surname")
    val n0 = createLabeledNode(Map("name" -> "Jake", "surname" -> "Soap"), "User")
    val n1 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Soap"), "User")
    val n2 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Smoke"), "User")
    for (_ <- 1 to 100) {
      createLabeledNode(Map("name" -> "Jake"), "User")
      createLabeledNode(Map("surname" -> "Soap"), "User")
      createLabeledNode("User")
    }
    resampleIndexes()
    List(n0, n1, n2)
  }
}
