/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.time.{LocalDate, LocalTime, OffsetTime, ZoneOffset}

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{ComparePlansWithAssertion, Configs, CypherComparisonSupport}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.values.storable.{CoordinateReferenceSystem, DurationValue, Values}
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.JavaConverters._

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
    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, "MATCH (n:User) WHERE n.surname = 'Soap' AND n.name = 'Joe' RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek(equality,equality)")
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
        plan should not(includeSomewhere.aPlan("NodeIndexSeek(equality,equality)"))
        plan should not(includeSomewhere.aPlan("NodeIndexSeek(equality,range)"))
        plan should not(includeSomewhere.aPlan("NodeIndexSeek(equality,exists)"))
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
    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, "MATCH (n:User) WHERE n.surname = 'Soap' AND n.name = 'Joe' RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek(equality,equality)").containingArgument(":User(name,surname)")
        plan should not(includeSomewhere.aPlan("NodeIndexSeek").containingArgument(":User(name)"))
        plan should not(includeSomewhere.aPlan("NodeIndexSeek").containingArgument(":User(surname)"))
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
        plan should not(includeSomewhere.aPlan("NodeIndexSeek(equality,equality)").containingArgument(":User(name,surname)"))
        plan should not(includeSomewhere.aPlan("NodeIndexSeek").containingArgument(":User(name)"))
        plan should includeSomewhere.aPlan("NodeIndexSeek").containingArgument(":User(surname)")
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
      ("n.name = 'Joe' AND n.surname STARTS WITH 'So'", "(equality,range)", false, false, "", Set(Map("n" -> n1))),
      ("n.name >= 'Jo' AND n.surname STARTS WITH 'So'", "(range,exists)", false, true, ".*cached\\[n.surname\\] STARTSWITH .*", Set(Map("n" -> n1))),
      ("n.name <= 'Je' AND exists(n.surname)", "(range,exists)", false, false, "", Set(Map("n" -> n3))),
      ("exists(n.name) AND exists(n.surname)", "", true, false, "", Set(Map("n" -> n1), Map("n" -> n2), Map("n" -> n3)))
    ).foreach {
      case (predicates, seekString, useScan, shouldFilter, filterArgument, resultSet) =>
        val query = s"MATCH (n:User) WHERE $predicates RETURN n"

        val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, query,
          planComparisonStrategy = ComparePlansWithAssertion(plan => {
            if (shouldFilter)
              plan should includeSomewhere.aPlan("Filter").containingArgumentRegex(filterArgument.r)
            else
              plan shouldNot includeSomewhere.aPlan("Filter")

            if (useScan)
              plan should includeSomewhere.aPlan("NodeIndexScan").containingArgument(":User(name,surname)")
            else
              plan should includeSomewhere.aPlan(s"NodeUniqueIndexSeek$seekString").containingArgument(":User(name,surname)")
          }))

        result.toComparableResult.toSet should equal(resultSet)
    }
  }

  test("should use composite index with combined equality and existence predicates") {
    // Given
    graph.createIndex("User", "name")
    graph.createIndex("User", "surname")
    graph.createIndex("User", "name", "surname")
    val n1 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Soap"), "User")
    val n2 = createLabeledNode(Map("name" -> "Joe", "surname" -> "Smoke"), "User")
    val n3 = createLabeledNode(Map("name" -> "Jake", "surname" -> "Soap"), "User")
    for (_ <- 1 to 100) {
      createLabeledNode(Map("name" -> "Jake"), "User")
      createLabeledNode(Map("surname" -> "Soap"), "User")
      createLabeledNode("User")
    }
    resampleIndexes()

    // When
    val resultEquality = executeWith(Configs.InterpretedAndSlottedAndMorsel, "MATCH (n:User) WHERE exists(n.surname) AND n.name = 'Jake' RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek(equality,exists)").containingArgument(":User(name,surname)")
        plan should not(includeSomewhere.aPlan("NodeIndexSeek").containingArgument(":User(name)"))
        plan should not(includeSomewhere.aPlan("NodeIndexSeek").containingArgument(":User(surname)"))
      }))

    // Then
    resultEquality.toComparableResult should equal(List(Map("n" -> n3)))

    // When
    val resultExists = executeWith(Configs.InterpretedAndSlottedAndMorsel, "MATCH (n:User) WHERE exists(n.surname) AND exists(n.name) RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexScan").containingArgument(":User(name,surname)")
        plan should not(includeSomewhere.aPlan("NodeIndexScan").containingArgument(":User(name)"))
        plan should not(includeSomewhere.aPlan("NodeIndexScan").containingArgument(":User(surname)"))
      }))

    // Then
    resultExists.toComparableResult.toSet should equal(Set(Map("n" -> n1), Map("n" -> n2), Map("n" -> n3)))
  }

  test("should be able to update composite index when only one property has changed") {
    graph.createIndex("Person", "name", "surname")
    val n = graph.execute("CREATE (n:Person {name:'Joe', surname:'Soap'}) RETURN n").columnAs("n").next().asInstanceOf[Node]
    graph.execute("MATCH (n:Person) SET n.surname = 'Bloggs'")
    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, "MATCH (n:Person) where n.name = 'Joe' and n.surname = 'Bloggs' RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek(equality,equality)")
      }))
    result.toComparableResult should equal(List(Map("n" -> n)))
  }

  test("should plan a composite index seek for a multiple property predicate expression when index is created after data") {
    executeWith(Configs.InterpretedAndSlotted, "WITH RANGE(0,10) AS num CREATE (:Person {id:num})") // ensure label cardinality favors index
    executeWith(Configs.InterpretedAndSlotted, "CREATE (n:Person {name:'Joe', surname:'Soap'})")
    graph.createIndex("Person", "name")
    graph.createIndex("Person", "name", "surname")
    executeWith(Configs.InterpretedAndSlottedAndMorsel, "MATCH (n:Person) WHERE n.name = 'Joe' AND n.surname = 'Soap' RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek(equality,equality)").containingArgument(":Person(name,surname)")
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
    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel,
      """MATCH (n:Foo)
        |WHERE n.bar IN [0,1,2,3,4,5,6,7,8,9]
        |  AND n.baz IN [0,1,2,3,4,5,6,7,8,9]
        |RETURN n.idx as x
        |ORDER BY x""".stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek(equality,equality)").containingArgument(":Foo(bar,baz)")
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
    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel,
      """MATCH (n:Foo)
        |WHERE n.bar = 1
        |  AND n.baz IN [0,1,2,3,4,5,6,7,8,9]
        |RETURN n.baz as x
        |ORDER BY x""".stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek(equality,equality)").containingArgument(":Foo(bar,baz)")
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
    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel,
      """MATCH (n:Foo)
        |WHERE n.baz = 1
        |  AND n.bar IN [0,1,2,3,4,5,6,7,8,9]
        |RETURN n.bar as x
        |ORDER BY x""".stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek(equality,equality)").containingArgument(":Foo(bar,baz)")
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
    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, "MATCH (n:L {foo: 42, bar: 1337, baz: 1980}) RETURN count(n)",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek(equality,equality,equality)").containingArgument(":L(foo,bar,baz)")
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
    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, "MATCH (n:L {foo: 42, bar: 1337, baz: 1980}) RETURN count(n)",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek(equality,equality,equality)").containingArgument(":L(foo,bar,baz)")
      }))
    result.toComparableResult should equal(Seq(Map("count(n)" -> 1)))
  }

  test("should not fail on multiple attempts to create a composite index") {
    // Given
    executeWith(Configs.All, "CREATE INDEX ON :Person(name, surname)")
    executeWith(Configs.All, "CREATE INDEX ON :Person(name, surname)")
  }

  test("should use range queries against a composite index") {
    // Given
    graph.createIndex("X", "p1", "p2")
    val n = createLabeledNode(Map("p1" -> 1, "p2" -> 1), "X")

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, "match (n:X) where n.p1 = 1 AND n.p2 > 0 return n;",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek(equality,range)").containingArgument(":X(p1,p2)")
      }))

    // Then
    result.toComparableResult should equal(Seq(Map("n" -> n)))
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
      ("n.name STARTS WITH 'B' AND exists(n.age)", ":User(name,age)", "(range,exists)", Set(Map("n" -> n2)), false, ""), // prefix
      ("n.name <= 'C' AND exists(n.active)", ":User(name,active)", "(range,exists)", Set(Map("n" -> n2), Map("n" -> n3)), false, ""), // less than
      ("n.age >= 18 AND exists(n.name)", ":User(age,name)", "(range,exists)", Set(Map("n" -> n1), Map("n" -> n2), Map("n" -> n4)), false, ""), // greater than
      ("n.name STARTS WITH 'B' AND n.age = 19", ":User(age,name)", "(equality,range)", Set.empty, false, ""), // prefix after equality
      ("n.age > 18 AND n.age < 60 AND exists(n.active)", ":User(age,active)", "(range,exists)", Set(Map("n" -> n1), Map("n" -> n4)), false, ""), // range between
      ("n.name = 'Jake' AND n.active > false", ":User(name,active)", "(equality,range)", Set(Map("n" -> n5)), false, ""), // greater than on boolean
      ("n.active < true AND exists(n.age)", ":User(active,age)", "(range,exists)", Set(Map("n" -> n2), Map("n" -> n3), Map("n" -> n4)), false, ""), // less than on boolean
      ("n.active < false AND exists(n.age)", ":User(active,age)", "(range,exists)", Set.empty, false, ""), // less than false
      ("n.active >= false AND n.active <= true AND n.age < 10", ":User(active,age)", "(range,exists)", Set(Map("n" -> n3)), true, ".*cached\\[n.age\\] < .*") // range between on boolean
    ).foreach {
      case (predicates, indexOn, seekString, resultSet, shouldFilter, filterArgument) =>
        // When
        val query = s"MATCH (n:User) WHERE $predicates RETURN n"
        val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, query,
          planComparisonStrategy = ComparePlansWithAssertion(plan => {
            if (shouldFilter)
              plan should includeSomewhere.aPlan("Filter").containingArgumentRegex(filterArgument.r)
            else
              plan shouldNot includeSomewhere.aPlan("Filter")

            plan should includeSomewhere.aPlan(s"NodeIndexSeek$seekString").containingArgument(indexOn)
          }))

        // Then
        result.toComparableResult.toSet should equal(resultSet)
    }
  }

  test("should use composite index for range, prefix, contains and exists predicates") {
    // TODO update when ends with and contains
    // Given
    graph.createIndex("User", "name", "surname", "age", "active")
    val n = createLabeledNode(Map("name" -> "joe", "surname" -> "soap", "age" -> 25, "active" -> true), "User")

    // For all combinations
    Seq(
      ("n.name = 'joe' AND n.surname = 'soap' AND n.age = 25 AND n.active = true", false, false, "(equality,equality,equality,equality)", Seq.empty), // all equality
      ("n.surname = 'soap' AND n.age = 25 AND n.active = true AND n.name = 'joe'", false, false, "(equality,equality,equality,equality)", Seq.empty), // different order
      ("n.name = 'joe' AND n.surname = 'soap' AND n.age = 25 AND exists(n.active)", false, false, "(equality,equality,equality,exists)", Seq.empty), // exists()
      ("n.name = 'joe' AND n.surname = 'soap' AND n.age >= 25 AND n.active = true", false, true, "(equality,equality,range,exists)", Seq(".*cached\\[n.active\\] = true.*".r)), // inequality
      ("n.name = 'joe' AND n.surname STARTS WITH 's' AND n.age = 25 AND n.active = true", false, true, "(equality,range,exists,exists)", Seq(".*cached\\[n.active\\] = true.*".r, ".*cached\\[n.age\\] = .*".r)), // prefix
//      ("n.name = 'joe' AND n.surname ENDS WITH 'p' AND n.age = 25 AND n.active = true", false, true, "(equality,?,exists,exists)", Seq(".*cached\\[n.active\\] = true.*".r, ".*cached\\[n.age\\] = .*".r)), // suffix
      ("n.name >= 'i' AND n.surname = 'soap' AND n.age = 25 AND n.active = true", false, true, "(range,exists,exists,exists)", Seq(".*cached\\[n.active\\] = true.*".r, ".*cached\\[n.surname\\] = .*".r, ".*cached\\[n.age\\] = .*".r)), // inequality first
      ("n.name STARTS WITH 'j' AND n.surname = 'soap' AND n.age = 25 AND n.active = true", false, true, "(range,exists,exists,exists)", Seq(".*cached\\[n.active\\] = true.*".r, ".*cached\\[n.surname\\] = .*".r, ".*cached\\[n.age\\] = .*".r)), // prefix first
//      ("n.name CONTAINS 'j' AND n.surname = 'soap' AND n.age = 25 AND n.active = true", false, true, "(?,exists,exists,exists)", Seq(".*cached\\[n.active\\] = true.*".r, ".*cached\\[n.surname\\] = .*".r, ".*cached\\[n.age\\] = .*".r)), // contains first
      ("n.name = 'joe' AND n.surname STARTS WITH 'soap' AND n.age <= 25 AND exists(n.active)", false, true, "(equality,range,exists,exists)", Seq(".*cached\\[n.age\\] <= .*".r)), // combination: equality, prefix, inequality, exists()

      ("n.name = 'joe' AND n.surname = 'soap' AND n.age = 25 AND n.active >= true", false, false, "(equality,equality,equality,range)", Seq.empty), // inequality last
      ("n.name = 'joe' AND n.surname = 'soap' AND n.age >= 25 AND n.active >= true", false, true, "(equality,equality,range,exists)", Seq(".*cached\\[n.active\\] >= true.*".r)), // inequality last two
      ("n.name = 'joe' AND n.surname = 'soap' AND n.age >= 25 AND exists(n.active)", false, false, "(equality,equality,range,exists)", Seq.empty), // combination: equality, equality, inequality, exists()
      ("n.name = 'joe' AND n.surname >= 'r' AND exists(n.age) AND exists(n.active)", false, false, "(equality,range,exists,exists)", Seq.empty), // combination: equality, inequality, exists(), exists()
      ("n.name = 'joe' AND exists(n.surname) AND exists(n.age) AND n.active = true", false, true, "(equality,exists,exists,exists)", Seq(".*cached\\[n.active\\] = true.*".r)), // combination: equality, exists(), exists(), equality
      ("n.name = 'joe' AND exists(n.surname) AND exists(n.age) AND exists(n.active)", false, false, "(equality,exists,exists,exists)", Seq.empty), // combination: equality, exists(), exists(), exists()
      ("n.name >= 'i' AND exists(n.surname) AND n.age >= 25 AND exists(n.active)", false, true, "(range,exists,exists,exists)", Seq(".*cached\\[n.age\\] >= .*".r)), // combination: inequality, exists(), inequality, exists()
      ("n.name >= 'i' AND exists(n.surname) AND exists(n.age) AND exists(n.active)", false, false, "(range,exists,exists,exists)", Seq.empty), // combination: inequality, exists(), exists(), exists()
      ("exists(n.name) AND n.surname = 'soap' AND n.age >= 25 AND exists(n.active)", true, true, "", Seq(".*cached\\[n.age\\] >= .*".r, ".*cached\\[n.surname\\] = .*".r)), // exists() first
      ("exists(n.name) AND exists(n.surname) AND exists(n.age) AND exists(n.active)", true, false, "", Seq.empty) // all exists
    ).foreach {
      case (predicates, useScan, shouldFilter, seekString, filterStrings) =>

        // When
        val query = s"MATCH (n:User) WHERE $predicates RETURN n"
        val result = try {
          executeWith(Configs.InterpretedAndSlottedAndMorsel, query,
            planComparisonStrategy = ComparePlansWithAssertion(plan => {
              //THEN
              if (shouldFilter) {
                plan should includeSomewhere.aPlan("Filter").containingArgumentRegex(filterStrings: _*)
                plan shouldNot includeSomewhere.aPlan("Filter").containingArgumentRegex(".*exists\\(cached\\[n\\..*\\]\\).*".r)
              } else
                plan shouldNot includeSomewhere.aPlan("Filter")

              if (useScan)
                plan should includeSomewhere.aPlan("NodeIndexScan").containingArgument(":User(name,surname,age,active)")
              else
                plan should includeSomewhere.aPlan(s"NodeIndexSeek$seekString").containingArgument(":User(name,surname,age,active)")
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

  test("should use composite index for is not null and regex") {
    //  TODO not equals

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
      ("n.name = 'Joe' AND n.surname IS NOT NULL", "(equality,exists)", Set(Map("name" -> "Joe Soap")), false, false, Seq.empty),
      ("n.name = 'Ivan' AND n.surname =~ 'S.*p'", "(equality,exists)", Set(Map("name" -> "Ivan Soap")), false, true, Seq(".*cached\\[n.surname\\] =~ .*".r)),
//      ("n.name = 'Ivan' AND n.surname <> 'Smoke'", "(equality,exists)", Set(Map("name" -> "Ivan Soap")), false, true, Seq(".*cached\\[n.surname\\] <> .*".r)),
      ("n.name >= 'J' AND n.surname IS NOT NULL", "(range,exists)", Set(Map("name" -> "Joe Soap")), false, false, Seq.empty),
      ("n.name <= 'J' AND n.surname =~ 'S.*p'", "(range,exists)", Set(Map("name" -> "Ivan Soap")), false, true, Seq(".*cached\\[n.surname\\] =~ .*".r)),
//      ("n.name <= 'J' AND n.surname <> 'Smoke'", "(range,exists)", Set(Map("name" -> "Ivan Soap")), false, true, Seq(".*cached\\[n.surname\\] <> .*".r)),
      ("exists(n.name) AND n.surname IS NOT NULL", "", Set(Map("name" -> "Joe Soap"), Map("name" -> "Ivan Soap"), Map("name" -> "Ivan Smoke")), true, false, Seq.empty),
      ("exists(n.name) AND n.surname =~ 'S.*p'", "", Set(Map("name" -> "Joe Soap"), Map("name" -> "Ivan Soap")), true, true, Seq(".*cached\\[n.surname\\] =~ .*".r)),
//      ("exists(n.name) AND n.surname <> 'Smoke'", "", Set(Map("name" -> "Joe Soap"), Map("name" -> "Ivan Soap")), true, true, Seq(".*cached\\[n.surname\\] <> .*".r)),

      ("n.name IS NOT NULL AND n.surname = 'Smoke'", "", Set(Map("name" -> "Ivan Smoke")), true, true, Seq(".*cached\\[n.surname\\] = .*".r)),
      ("n.name IS NOT NULL AND n.surname =~ 'Sm.*e'", "", Set(Map("name" -> "Ivan Smoke")), true, true, Seq(".*cached\\[n.surname\\] =~ .*".r)),
//      ("n.name IS NOT NULL AND n.surname <> 'Soap'", "", Set(Map("name" -> "Ivan Smoke")), true, true, Seq(".*cached\\[n.surname\\] =~ .*".r)),
      ("n.name IS NOT NULL AND n.surname < 'So'", "", Set(Map("name" -> "Ivan Smoke")), true, true, Seq(".*cached\\[n.surname\\] < .*".r)),
      ("n.name IS NOT NULL AND exists(n.surname)", "", Set(Map("name" -> "Joe Soap"), Map("name" -> "Ivan Soap"), Map("name" -> "Ivan Smoke")), true, false, Seq.empty),
      ("n.name IS NOT NULL AND n.surname IS NOT NULL", "", Set(Map("name" -> "Joe Soap"), Map("name" -> "Ivan Soap"), Map("name" -> "Ivan Smoke")), true, false, Seq.empty),
      ("n.name =~ 'J.*' AND n.surname = 'Smoke'", "", Set.empty, true, true, Seq(".*cached\\[n.name\\] =~ .*".r, ".*cached\\[n.surname\\] = .*".r)),
      ("n.name =~ 'I.*' AND n.surname =~ 'S.*p'", "", Set(Map("name" -> "Ivan Soap")), true, true, Seq(".*cached\\[n.name\\] =~ .*".r, ".*cached\\[n.surname\\] =~ .*".r)),
//      ("n.name =~ 'J.*' AND n.surname <> 'Soap'", "", Set.empty, true, true, Seq(".*cached\\[n.name\\] =~ .*".r, ".*cached\\[n.surname\\] = .*".r)),
      ("n.name =~ 'I.*' AND n.surname > 'Sn'", "", Set(Map("name" -> "Ivan Soap")), true, true, Seq(".*cached\\[n.name\\] =~ .*".r, ".*cached\\[n.surname\\] > .*".r)),
      ("n.name =~ 'J.*' AND exists(n.surname)", "", Set(Map("name" -> "Joe Soap")), true, true, Seq(".*cached\\[n.name\\] =~ .*".r)),
      ("n.name =~ 'J.*' AND n.surname IS NOT NULL", "", Set(Map("name" -> "Joe Soap")), true, true, Seq(".*cached\\[n.name\\] =~ .*".r))
//      ("n.name <> 'Joe' AND n.surname = 'Soap'", "", Set(Map("name" -> "Ivan Soap")), true, true, Seq(".*cached\\[n.name\\] =~ .*".r, ".*cached\\[n.surname\\] =~ .*".r)),
//      ("n.name <> 'Joe' AND n.surname =~ '.*e'", "", Set(Map("name" -> "Ivan Smoke")), true, true, Seq(".*cached\\[n.name\\] =~ .*".r, ".*cached\\[n.surname\\] =~ .*".r)),
//      ("n.name <> 'Joe' AND n.surname <> 'Smoke'", "", Set(Map("name" -> "Ivan Soap")), true, true, Seq(".*cached\\[n.name\\] =~ .*".r, ".*cached\\[n.surname\\] =~ .*".r)),
//      ("n.name <> 'Joe' AND n.surname >= 'S'", "", Set(Map("name" -> "Ivan Soap"), Map("name" -> "Ivan Smoke")), true, true, Seq(".*cached\\[n.name\\] =~ .*".r, ".*cached\\[n.surname\\] =~ .*".r)),
//      ("n.name <> 'Ivan' AND exists(n.surname)", "", Set(Map("name" -> "Joe Soap")), true, true, Seq(".*cached\\[n.name\\] =~ .*".r)),
//      ("n.name <> 'Ivan' AND n.surname IS NOT NULL", "", Set(Map("name" -> "Joe Soap")), true, true, Seq(".*cached\\[n.name\\] =~ .*".r))

    ).foreach {
      case (predicates, seekString, resultSet, useScan, shouldFilter, filterArguments) =>
        // When
        val query = s"MATCH (n:User) WHERE $predicates RETURN (n.name + ' ' + n.surname) AS name"
        val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, query,
          planComparisonStrategy = ComparePlansWithAssertion(plan => {
            if (shouldFilter)
              plan should includeSomewhere.aPlan("Filter").containingArgumentRegex(filterArguments: _*)
            else
              plan shouldNot includeSomewhere.aPlan("Filter")

            if (useScan)
              plan should includeSomewhere.aPlan("NodeIndexScan").containingArgument(":User(name,surname)")
            else
              plan should includeSomewhere.aPlan(s"NodeIndexSeek$seekString").containingArgument(":User(name,surname)")
          }))

        // Then
        result.toComparableResult.toSet should equal(resultSet)
    }
  }

  test("nested index join with composite indexes") {
    // given
    graph.createIndex("X", "p1", "p2")
    (1 to 1000) foreach { _ => // Get the planner to do what we expect it to!
      createLabeledNode("X")
    }
    val a = createNode("p1" -> 1, "p2" -> 1)
    val b = createLabeledNode(Map("p1" -> 1, "p2" -> 1), "X")

    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, "match (a), (b:X) where id(a) = $id AND b.p1 = a.p1 AND b.p2 = 1 return b",
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek(equality,equality)").containingArgument(":X(p1,p2)")
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

    val resultNoIndex = executeWith(Configs.InterpretedAndSlottedAndMorsel, query)

    graph.createIndex("User", "name", "city")
    resampleIndexes()

    val resultIndex = executeWith(Configs.InterpretedAndSlottedAndMorsel, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek(equality,equality)")
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

    val resultNoIndex = executeWith(Configs.InterpretedAndSlottedAndMorsel, query)

    graph.createIndex("User", "name", "city", "age")
    resampleIndexes()

    val resultIndex = executeWith(Configs.InterpretedAndSlottedAndMorsel, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("Filter").containingArgumentRegex(".*distance\\(cached\\[n.city\\], point.*".r, ".*cached\\[n.age\\] >.*".r)
        plan should includeSomewhere.aPlan("NodeIndexSeek(equality,range,exists)")
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

    val resultNoIndex = executeWith(Configs.InterpretedAndSlottedAndMorsel, query)

    graph.createIndex("Label", "date", "time")
    resampleIndexes()

    val resultIndex = executeWith(Configs.InterpretedAndSlottedAndMorsel, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek(equality,equality)")
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

    val resultNoIndex = executeWith(Configs.InterpretedAndSlottedAndMorsel, query)

    graph.createIndex("Runner", "name", "result")
    resampleIndexes()

    val resultIndex = executeWith(Configs.InterpretedAndSlottedAndMorsel, query,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        //THEN
        plan should includeSomewhere.aPlan("NodeIndexSeek(equality,equality)")
      }))

    // Then
    resultNoIndex.toComparableResult should equal(List(Map("n" -> n1)))
    resultIndex.toComparableResult should equal(resultNoIndex.toComparableResult)
  }

  case class haveIndexes(expectedIndexes: String*) extends Matcher[GraphDatabaseQueryService] {
    def apply(graph: GraphDatabaseQueryService): MatchResult = {
      graph.inTx {
        val indexNames = graph.schema().getIndexes.asScala.toList.map(i => s":${i.getLabel}(${i.getPropertyKeys.asScala.toList.mkString(",")})")
        val result = expectedIndexes.forall(i => indexNames.contains(i.toString))
        MatchResult(
          result,
          s"Expected graph to have indexes ${expectedIndexes.mkString(", ")}, but it was ${indexNames.mkString(", ")}",
          s"Expected graph to not have indexes ${expectedIndexes.mkString(", ")}, but it did."
        )
      }
    }
  }

}
