/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.exceptions.SyntaxException
import org.neo4j.internal.cypher.acceptance.comparisonsupport._

class HelpfulErrorMessagesTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should provide sensible error message when omitting colon before relationship type on create") {

    failWithError(Configs.All,

      "CREATE (a)-[ASSOCIATED_WITH]->(b)",
      Seq("Exactly one relationship type must be specified for CREATE. Did you forget to prefix your relationship type with a ':'?"))
  }

  test("should provide sensible error message when trying to add multiple relationship types on create") {
    failWithError(Configs.All,
      "CREATE (a)-[:ASSOCIATED_WITH|:KNOWS]->(b)",
      Seq("A single relationship type must be specified for CREATE",
          "The given query is not currently supported in the selected cost-based planner" ))
  }

  test("should provide sensible error message when omitting colon before relationship type on merge") {
    failWithError(Configs.All,
      "MERGE (a)-[ASSOCIATED_WITH]->(b)",
      Seq("Exactly one relationship type must be specified for MERGE. Did you forget to prefix your relationship type with a ':'?"))
  }

  test("should provide sensible error message when trying to add multiple relationship types on merge") {
    failWithError(Configs.All,
      "MERGE (a)-[:ASSOCIATED_WITH|:KNOWS]->(b)",
      Seq("A single relationship type must be specified for MERGE",
      "The given query is not currently supported in the selected cost-based planner"))
  }

  test("should provide sensible error message for invalid regex syntax together with index") {
    executeSingle("CREATE (n:Person {text:'abcxxxdefyyyfff'})")
    failWithError(Configs.CachedProperty,
      "MATCH (x:Person) WHERE x.text =~ '*xxx*yyy*' RETURN x.text", List("Invalid Regex:"))
  }

  test("should provide sensible error message for START in newer runtimes") {
    val query = "START n=node(0) RETURN n"
    failWithError(Configs.All, query, Seq(
      "The given query is not currently supported in the selected runtime",
      "The given query is not currently supported in the selected cost-based planner",
      "START is deprecated"))
  }

  test("should provide sensible error message for CREATE UNIQUE in newer runtimes") {
    val query = "MATCH (root { name: 'root' }) CREATE UNIQUE (root)-[:LOVES]-(someone) RETURN someone"
    failWithError(Configs.All, query, Seq(
      "The given query is not currently supported in the selected runtime",
      "The given query is not currently supported in the selected cost-based planner",
      "CREATE UNIQUE is no longer supported"
    ))
  }

  test("should provide sensible error message for removed toInt() function") {
    val query = "RETURN toInt('1')"
    failWithError(Configs.All, query, Seq("The function toInt() is no longer supported. Please use toInteger() instead"))
  }

  test("should provide sensible error message for removed lower() function") {
    val query = "RETURN lower('BAR')"
    failWithError(Configs.All, query, Seq("The function lower() is no longer supported. Please use toLower() instead"))
  }

  test("should provide sensible error message for removed upper() function") {
    val query = "RETURN upper('foo')"
    failWithError(Configs.All, query, Seq("The function upper() is no longer supported. Please use toUpper() instead"))
  }

  test("should provide sensible error message for removed rels() function") {
    val query = "MATCH p = ()-->() RETURN rels(p) AS r"
    failWithError(Configs.All, query, Seq("The function rels() is no longer supported. Please use relationships() instead"))
  }

  test("should provide sensible error message for filter") {
    val query = "WITH [1,2,3] AS list RETURN filter(x IN list WHERE x % 2 = 1) AS odds"
    failWithError(Configs.All, query, Seq("Filter is no longer supported. Please use list comprehension instead"))
  }

  test("should provide sensible error message for extract") {
    val query = "WITH [1,2,3] AS list RETURN extract(x IN list | x * 10) AS tens"
    failWithError(Configs.All, query, Seq("Extract is no longer supported. Please use list comprehension instead"))
  }

  test("should provide sensible error message for old parameter syntax") {
    val query = "RETURN {param} as parameter"
    failWithError(Configs.All, query, Seq("The old parameter syntax `{param}` is no longer supported. Please use `$param` instead"))
  }

  test("should give correct error message with invalid number literal in a subtract") {
    a[SyntaxException] shouldBe thrownBy {
      executeSingle("with [1a-1] as list return list", Map())
    }
  }

  // Operations on incompatible types
  test("should provide sensible error message when trying to add incompatible types") {
    // We want to deliberately fail after semantic checking (at runtime), thus the need for CREATE

    executeSingle("CREATE (n:Test {" +
      "loc: point({x:22, y:44}), " +
      "num: 2, dur: duration({ days: 1, hours: 12 }), " +
      "dat: datetime('2015-07-21T21:40:32.142+0100'), " +
      "bool: true, " +
      "flo: 2.9 })")

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.num + n.loc", List("Cannot add `Long` and `Point`"))

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.num + n.dur", List("Cannot add `Long` and `Duration`"))

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.num + n.dat", List("Cannot add `Long` and `DateTime`"))

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.flo + n.bool", List("Cannot add `Double` and `Boolean`"))
  }

  test("should provide sensible error message when trying to multiply incompatible types") {
    // We want to deliberately fail after semantic checking (at runtime), thus the need for CREATE

    executeSingle("CREATE (n:Test {" +
      "loc: point({x:22, y:44}), " +
      "num: 2, dur: duration({ days: 1, hours: 12 }), " +
      "dat: datetime('2015-07-21T21:40:32.142+0100'), " +
      "bool: true, " +
      "flo: 2.9," +
      "lst: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], " +
      "str: 's' })")

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.num * n.loc", List("Cannot multiply `Long` and `Point`"))

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.num * n.dat", List("Cannot multiply `Long` and `DateTime`"))

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.flo * n.bool", List("Cannot multiply `Double` and `Boolean`"))

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.lst * n.str", List("Cannot multiply `LongArray` and `String`"))
  }

  test("should provide sensible error message when trying to subtract incompatible types") {
    // We want to deliberately fail after semantic checking (at runtime), thus the need for CREATE

    executeSingle("CREATE (n:Test {" +
      "loc: point({x:22, y:44}), " +
      "num: 2, dur: duration({ days: 1, hours: 12 }), " +
      "dat: datetime('2015-07-21T21:40:32.142+0100'), " +
      "bool: true, " +
      "flo: 2.9," +
      "lst: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], " +
      "str: 's' })")

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.num - n.loc", List("Cannot subtract `Point` from `Long`"))

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.num - n.dat", List("Cannot subtract `DateTime` from `Long`"))

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.flo - n.bool", List("Cannot subtract `Boolean` from `Double`"))

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.lst - n.str", List("Cannot subtract `String` from `LongArray`"))
  }

  test("should provide sensible error message when trying to calculate modulus of incompatible types") {
    // We want to deliberately fail after semantic checking (at runtime), thus the need for CREATE

    executeSingle("CREATE (n:Test {" +
      "loc: point({x:22, y:44}), " +
      "num: 2, dur: duration({ days: 1, hours: 12 }), " +
      "dat: datetime('2015-07-21T21:40:32.142+0100'), " +
      "bool: true, " +
      "flo: 2.9," +
      "lst: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], " +
      "str: 's' })")

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.num % n.loc", List("Cannot calculate modulus of `Long` and `Point`"))

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.num % n.dat", List("Cannot calculate modulus of `Long` and `DateTime`"))

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.flo % n.bool", List("Cannot calculate modulus of `Double` and `Boolean`"))

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.lst % n.str", List("Cannot calculate modulus of `LongArray` and `String`"))
  }

  test("should provide sensible error message when trying to divide incompatible types") {
    // We want to deliberately fail after semantic checking (at runtime), thus the need for CREATE

    executeSingle("CREATE (n:Test {" +
      "loc: point({x:22, y:44}), " +
      "num: 2, dur: duration({ days: 1, hours: 12 }), " +
      "dat: datetime('2015-07-21T21:40:32.142+0100'), " +
      "bool: true, " +
      "flo: 2.9," +
      "lst: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], " +
      "str: 's' })")

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.num / n.loc", List("Cannot divide `Long` by `Point`"))

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.num / n.dat", List("Cannot divide `Long` by `DateTime`"))

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.flo / n.bool", List("Cannot divide `Double` by `Boolean`"))

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.lst / n.str", List("Cannot divide `LongArray` by `String`"))
  }

  test("should provide sensible error message when trying to raise to the power of incompatible types") {
    // We want to deliberately fail after semantic checking (at runtime), thus the need for CREATE

    executeSingle("CREATE (n:Test {" +
      "loc: point({x:22, y:44}), " +
      "num: 2, dur: duration({ days: 1, hours: 12 }), " +
      "dat: datetime('2015-07-21T21:40:32.142+0100'), " +
      "bool: true, " +
      "flo: 2.9," +
      "lst: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], " +
      "str: 's' })")

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.num ^ n.loc", List("Cannot raise `Long` to the power of `Point`"))

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.num ^ n.dat", List("Cannot raise `Long` to the power of `DateTime`"))

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.flo ^ n.bool", List("Cannot raise `Double` to the power of `Boolean`"))

    failWithError(Configs.All,
      "MATCH (n:Test) RETURN n.lst ^ n.str", List("Cannot raise `LongArray` to the power of `String`"))
  }

  test("should provide sensible error message for using compiled expression with interpreted") {
    intercept[Exception](executeSingle("CYPHER runtime=interpreted expressionEngine=compiled RETURN 1")).getMessage should be("Cannot combine EXPRESSION ENGINE 'compiled' with RUNTIME 'interpreted'")
  }

  test("should provide sensible error message for using compiled expression with compiled runtime") {
    intercept[Exception](executeSingle("CYPHER runtime=compiled expressionEngine=compiled RETURN 1")).getMessage should be("Cannot combine EXPRESSION ENGINE 'compiled' with RUNTIME 'compiled'")
  }

  test("should provide sensible error message for using compiled operator engine with compiled runtime") {
    intercept[Exception](executeSingle("CYPHER runtime=compiled operatorEngine=compiled RETURN 1")).getMessage should be("Cannot combine OPERATOR ENGINE 'compiled' with RUNTIME 'compiled'")
  }

  test("should provide sensible error message for using compiled operator engine with slotted runtime") {
    intercept[Exception](executeSingle("CYPHER runtime=slotted operatorEngine=compiled RETURN 1")).getMessage should be("Cannot combine OPERATOR ENGINE 'compiled' with RUNTIME 'slotted'")
  }

  test("should provide sensible error message for using compiled operator engine with interpreted runtime") {
    intercept[Exception](executeSingle("CYPHER runtime=interpreted operatorEngine=compiled RETURN 1")).getMessage should be("Cannot combine OPERATOR ENGINE 'compiled' with RUNTIME 'interpreted'")
  }

  test("should provide sensible error message for using interpreted pipes fallback with compiled runtime") {
    intercept[Exception](executeSingle("CYPHER runtime=compiled interpretedPipesFallback=all RETURN 1")).getMessage should be("Cannot combine INTERPRETED PIPES FALLBACK 'all' with RUNTIME 'compiled'")
  }

  test("should provide sensible error message for using interpreted pipes fallback with slotted runtime") {
    intercept[Exception](executeSingle("CYPHER runtime=slotted interpretedPipesFallback=all RETURN 1")).getMessage should be("Cannot combine INTERPRETED PIPES FALLBACK 'all' with RUNTIME 'slotted'")
  }

  test("should provide sensible error message for using interpreted pipes fallback with interpreted runtime") {
    intercept[Exception](executeSingle("CYPHER runtime=interpreted interpretedPipesFallback=all RETURN 1")).getMessage should be("Cannot combine INTERPRETED PIPES FALLBACK 'all' with RUNTIME 'interpreted'")
  }

  test("should be able to use compiled expression engine with slotted") {
    inTx( tx =>
      tx.execute("CYPHER runtime=slotted expressionEngine=compiled RETURN 1").resultAsString() should not be null
    )
  }
}
