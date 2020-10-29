/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class ListExpressionAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should reduce on null accumulator") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query = "RETURN" +
        " reduce(res=null, x in [1] | CASE WHEN res IS NULL THEN x ELSE res END) as firstOfOne," +
        " reduce(res=null, x in [1,2] | CASE WHEN res IS NULL THEN x ELSE res END) as firstOfTwo," +
        " reduce(res=null, x in [null,2] | CASE WHEN res IS NULL THEN x ELSE res END) as firstNonNull," +
        " reduce(res=null, x in [] | CASE WHEN res IS NULL THEN x ELSE res END) as nullOfEmptyList")

    result.toList.head should equal(Map(
      "firstOfOne" -> 1,
      "firstOfTwo" -> 1,
      "firstNonNull" -> 2,
      "nullOfEmptyList" -> null))
  }

  test("should reduce on values") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query = "RETURN" +
        " reduce(acc=0, s IN ['1','22','1','333'] | acc + size(s)) AS result," +
        " reduce(acc=0, s IN ['1','22','1','333'] | acc + null) AS nullExpression," +
        " reduce(acc=0, s IN ['1',null,'1','333'] | acc + size(s)) AS nullElement," +
        " reduce(acc=7, s IN [] | 7 + s) AS emptyList," +
        " reduce(acc=null, s IN [] | 7 + s) AS emptyListOnNull")

    result.toList.head should equal(Map(
      "result" -> 7,
      "nullExpression" -> null,
      "nullElement" -> null,
      "emptyList" -> 7,
      "emptyListOnNull" -> null))
  }

  test("should reduce on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "RETURN" +
          " reduce(acc=0, n IN nodes(p) | acc + n.x) AS result," +
          " reduce(acc=0, n IN nodes(p) | acc + null) AS nullExpression," +
          " reduce(acc=0, n IN nodes(p) + [null] | acc + n.x) AS nullElement")

    result.toList.head should equal(Map(
      "result" -> 6,
      "nullExpression" -> null,
      "nullElement" -> null
    ))
  }

  test("should perform extracting list comprehension on values") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query = "RETURN" +
        " [s IN ['1','22','1','333'] | size(s)] AS result," +
        " [s IN ['1','22','1','333'] | null] AS nullExpression," +
        " [s IN ['1',null,'1','333'] | size(s)] AS nullElement," +
        " [s IN [] | 7 + s] AS emptyList")

    result.toList.head should equal(Map(
      "result" -> List(1, 2, 1, 3),
      "nullExpression" -> List(null, null, null, null),
      "nullElement" -> List(1, null, 1, 3),
      "emptyList" -> List()))
  }

  test("should perform extracting list comprehension on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "RETURN" +
          " [n IN nodes(p) | n.x] AS result," +
          " [n IN nodes(p) | null] AS nullExpression," +
          " [n IN nodes(p) + [null] | n.x] AS nullElement")

    result.toList.head should equal(Map(
      "result" -> List(1, 2, 3),
      "nullExpression" -> List(null, null, null),
      "nullElement" -> List(1, 2, 3, null)
    ))
  }

  test("should perform filtering list comprehension on values") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query = "RETURN" +
        " [s IN ['1','22','1','333'] WHERE s STARTS WITH '1'] AS result," +
        " [s IN ['1','22','1','333'] WHERE null] AS nullPredicate," +
        " [s IN ['1',null,'1','333'] WHERE size(s)>1] AS nullElement," +
        " [s IN [] WHERE s > 7] AS emptyList")

    result.toList.head should equal(Map(
      "result" -> List("1", "1"),
      "nullPredicate" -> List(),
      "nullElement" -> List("333"),
      "emptyList" -> List()
    ))
  }

  test("should perform filtering list comprehension on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "RETURN" +
          " [n IN nodes(p) WHERE n.x <= 2] AS result," +
          " [n IN nodes(p) WHERE null] AS nullPredicate," +
          " [n IN nodes(p) + [null] WHERE n.x < 2] AS nullElement")

    result.toList.head should equal(Map(
      "result" -> List(n1, n2),
      "nullPredicate" -> List(),
      "nullElement" -> List(n1)
    ))
  }

  test("should perform list comprehension on values") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query = "RETURN [s IN ['1','22','1','333']] AS result")

    result.toList.head should equal(Map("result" -> List("1", "22", "1", "333")))
  }

  test("should perform list comprehension on values, with predicate") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query = "RETURN [s IN ['1','22','1','333'] WHERE s STARTS WITH '1'] AS result")

    result.toList.head should equal(Map("result" -> List("1", "1")))
  }

  test("should perform comprehension on values, with predicate and extract") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query = "RETURN" +
        " [s IN ['1','22','1','333'] WHERE s STARTS WITH '1' | size(s)] AS result," +
        " [s IN ['1','22','1','333'] WHERE null | size(s)] AS nullPredicate," +
        " [s IN ['1',null,'1','333'] WHERE size(s)>0 | size(s)] AS nullElement," +
        " [s IN ['1','22','1','333'] WHERE size(s)>1 | null] AS nullExtract")

    result.toList.head should equal(Map(
      "result" -> List(1, 1), // ['1', '1']
      "nullPredicate" -> List(), // []
      "nullElement" -> List(1, 1, 3), // ['1', '1', '333']
      "nullExtract" -> List(null, null) // ['22', '333']
    ))
  }

  test("should perform list comprehension on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "RETURN [n IN nodes(p)] AS result")

    result.toList.head should equal(Map("result" -> List(n1, n2, n3)))
  }

  test("should perform list comprehension on nodes, with predicate") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "RETURN [n IN nodes(p) WHERE n.x <= 2] AS result")

    result.toList.head should equal(Map("result" -> List(n1, n2)))
  }

  test("should perform list comprehension on nodes, with predicate and extract") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "RETURN [n IN nodes(p) WHERE n.x <= 2 | n.x] AS result")

    result.toList.head should equal(Map("result" -> List(1, 2)))
  }

  test("should all predicate on values") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query = "RETURN " +
        " all(s IN ['1','22','1','333'] WHERE size(s) > 0) AS allTrue, " +
        " all(s IN ['1','22','1','333'] WHERE size(s) > 1) AS someFalse, " +
        " all(s IN ['1','22','1','333'] WHERE null) AS nullPredicate," +
        " all(s IN ['1',null,'1','333'] WHERE size(s) > 0) AS allTrueWithNull," +
        " all(s IN ['1',null,'1','333'] WHERE size(s) > 1) AS someFalseWithNull," +
        " all(s IN [] WHERE true) AS emptyList")

    result.toList.head should equal(Map(
      "allTrue" -> true,
      "someFalse" -> false,
      "nullPredicate" -> null,
      "allTrueWithNull" -> null,
      "someFalseWithNull" -> false,
      "emptyList" -> true))
  }

  test("should all predicate on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "WHERE all(n IN nodes(p) WHERE n:Label)" +
          "RETURN " +
          " all(n IN nodes(p) WHERE n.x > 0) AS allTrue, " +
          " all(n IN nodes(p) WHERE n.x > 1) AS someFalse," +
          " all(n IN nodes(p) WHERE null) AS nullPredicate," +
          " all(n IN nodes(p) + [null] WHERE n.x > 0) AS allTrueWithNull," +
          " all(n IN nodes(p) + [null] WHERE n.x > 1) AS someFalseWithNull")

    result.toList.head should equal(Map(
      "allTrue" -> true,
      "someFalse" -> false,
      "nullPredicate" -> null,
      "allTrueWithNull" -> null,
      "someFalseWithNull" -> false))
  }

  test("should all predicate on relationships") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2, "T", Map("x" -> 1))
    relate(n2, n3, "T", Map("x" -> 2))
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "WHERE all(r IN relationships(p) WHERE r:T)" +
          "RETURN " +
          " all(r IN relationships(p) WHERE r.x > 0) AS allTrue, " +
          " all(r IN relationships(p) WHERE r.x > 1) AS someFalse," +
          " all(r IN relationships(p) WHERE null) AS nullPredicate," +
          " all(r IN relationships(p) + [null] WHERE r.x > 0) AS allTrueWithNull," +
          " all(r IN relationships(p) + [null] WHERE r.x > 1) AS someFalseWithNull")

    result.toList.head should equal(Map(
      "allTrue" -> true,
      "someFalse" -> false,
      "nullPredicate" -> null,
      "allTrueWithNull" -> null,
      "someFalseWithNull" -> false))
  }

  test("should all predicate on relationships when not all types match") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2, "S", Map("x" -> 1))
    relate(n2, n3, "T", Map("x" -> 2))
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "WHERE all(r IN relationships(p) WHERE r:T)" +
          "RETURN p")

    result.toList shouldBe empty
  }

  test("should any predicate on values") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query = "RETURN " +
        " any(s IN ['1','22','1','333'] WHERE size(s) = 1) AS someTrue, " +
        " any(s IN ['1','22','1','333'] WHERE size(s) = 0) AS allFalse, " +
        " any(s IN ['1','22','1','333'] WHERE null) AS nullPredicate," +
        " any(s IN ['1',null,'1','333'] WHERE size(s) = 1) AS someTrueWithNull," +
        " any(s IN ['1',null,'1','333'] WHERE size(s) = 0) AS allFalseWithNull," +
        " any(s IN [] WHERE s > 7) AS emptyList")

    result.toList.head should equal(Map(
      "someTrue" -> true,
      "allFalse" -> false,
      "nullPredicate" -> null,
      "someTrueWithNull" -> true,
      "allFalseWithNull" -> null,
      "emptyList" -> false))
  }

  test("should any predicate on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "WHERE any(n IN nodes(p) WHERE n:Label) " +
          "RETURN " +
          " any(n IN nodes(p) WHERE n.x = 1) AS someTrue," +
          " any(n IN nodes(p) WHERE n.x = 0) AS allFalse," +
          " any(n IN nodes(p) WHERE null) AS nullPredicate," +
          " any(n IN nodes(p)+[null] WHERE n.x = 1) AS someTrueWithNull," +
          " any(n IN nodes(p)+[null] WHERE n.x = 0) AS allFalseWithNull")

    result.toList.head should equal(Map(
      "someTrue" -> true,
      "allFalse" -> false,
      "nullPredicate" -> null,
      "someTrueWithNull" -> true,
      "allFalseWithNull" -> null))
  }

  test("should any predicate on relationships") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2, "T", Map("x" -> 1))
    relate(n2, n3, "T", Map("x" -> 2))
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "WHERE any(r IN relationships(p) WHERE r:T) " +
          "RETURN " +
          " any(r IN relationships(p) WHERE r.x = 1) AS someTrue," +
          " any(r IN relationships(p) WHERE r.x = 0) AS allFalse," +
          " any(r IN relationships(p) WHERE null) AS nullPredicate," +
          " any(r IN relationships(p)+[null] WHERE r.x = 1) AS someTrueWithNull," +
          " any(r IN relationships(p)+[null] WHERE r.x = 0) AS allFalseWithNull")

    result.toList.head should equal(Map(
      "someTrue" -> true,
      "allFalse" -> false,
      "nullPredicate" -> null,
      "someTrueWithNull" -> true,
      "allFalseWithNull" -> null))
  }

  test("should none predicate on values") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query = "RETURN" +
        " none(s IN ['1','22','1','333'] WHERE size(s) = 0) AS allFalse," +
        " none(s IN ['1','22','1','333'] WHERE size(s) = 1) AS someTrue," +
        " none(s IN ['1','22','1','333'] WHERE null) AS nullPredicate," +
        " none(s IN ['1',null,'1','333'] WHERE size(s) = 0) AS allFalseWithNull," +
        " none(s IN ['1',null,'1','333'] WHERE size(s) = 1) AS someTrueWithNull," +
        " none(s IN [] WHERE s > 7) AS emptyList")

    result.toList.head should equal(Map(
      "allFalse" -> true,
      "someTrue" -> false,
      "nullPredicate" -> null,
      "allFalseWithNull" -> null,
      "someTrueWithNull" -> false,
      "emptyList" -> true
    ))
  }

  test("should none predicate on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "WHERE none(n IN nodes(p) WHERE n:Fake) " +
          "RETURN " +
          " none(n IN nodes(p) WHERE n.x = 0) AS allFalse," +
          " none(n IN nodes(p) WHERE n.x = 1) AS someTrue, " +
          " none(n IN nodes(p) WHERE null) AS nullValue," +
          " none(n IN nodes(p) + [null] WHERE n.x = 0) AS allFalseWithNull," +
          " none(n IN nodes(p) + [null] WHERE n.x = 1) AS someTrueWithNull")

    result.toList.head should equal(Map(
      "allFalse" -> true,
      "someTrue" -> false,
      "nullValue" -> null,
      "allFalseWithNull" -> null,
      "someTrueWithNull" -> false
    ))
  }

  test("should none predicate on relationships") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2, "T", Map("x" -> 1))
    relate(n2, n3, "T", Map("x" -> 2))
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "WHERE none(r IN relationships(p) WHERE r:Fake) " +
          "RETURN " +
          " none(r IN relationships(p) WHERE r.x = 0) AS allFalse," +
          " none(r IN relationships(p) WHERE r.x = 1) AS someTrue, " +
          " none(r IN relationships(p) WHERE null) AS nullValue," +
          " none(r IN relationships(p) + [null] WHERE r.x = 0) AS allFalseWithNull," +
          " none(r IN relationships(p) + [null] WHERE r.x = 1) AS someTrueWithNull")

    result.toList.head should equal(Map(
      "allFalse" -> true,
      "someTrue" -> false,
      "nullValue" -> null,
      "allFalseWithNull" -> null,
      "someTrueWithNull" -> false
    ))
  }

  test("should single predicate on values") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query = "RETURN " +
        " single(s IN ['1','22','1','333'] WHERE s = '0') AS noneTrue," +
        " single(s IN ['1','22','1','333'] WHERE s = '333') AS oneTrue," +
        " single(s IN ['1','22','1','333'] WHERE s = '1') AS twoTrue," +
        " single(s IN ['1','22','1','333'] WHERE null) AS nullPredicate," +
        " single(s IN ['1',null,'1','333'] WHERE s = '0') AS noneTrueWithNull," +
        " single(s IN ['1',null,'1','333'] WHERE s = '333') AS oneTrueWithNull," +
        " single(s IN [] WHERE true) AS emptyList")

    result.toList.head should equal(Map(
      "noneTrue" -> false,
      "oneTrue" -> true,
      "twoTrue" -> false,
      "nullPredicate" -> null,
      "noneTrueWithNull" -> null,
      "oneTrueWithNull" -> null,
      "emptyList" -> false))
  }

  // NOTE: should be merged with above test, but older Cypher versions fail on ONLY this case. it would be a shame to remove asserts on all other cases.
  test("should single predicate on values -- multiple true with null case") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query = "RETURN " +
        " single(s IN ['1',null,'1','333'] WHERE s = '1') AS twoTrueWithNull")

    result.toList.head should equal(Map(
      "twoTrueWithNull" -> false))
  }

  test("should single predicate on nodes") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    relate(n1, n2)
    relate(n2, n3)
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query =
        "MATCH p=(n1:Label {x:1})-[*2]-(n3:Label {x:3}) " +
          "WHERE single(n IN nodes(p) WHERE n.x = 1) " +
          "RETURN " +
          " single(n IN nodes(p) WHERE n.x = 0) AS noneTrue," +
          " single(n IN nodes(p) WHERE n.x = 1) AS oneTrue, " +
          " single(n IN nodes(p) WHERE n.x <= 2) AS twoTrue, " +
          " single(n IN nodes(p) WHERE null) AS nullPredicate," +
          " single(n IN nodes(p) + [null] WHERE n.x = 0) AS noneTrueWithNull," +
          " single(n IN nodes(p) + [null] WHERE n.x = 1) AS oneTrueWithNull," +
          " single(n IN nodes(p) + [null] WHERE n.x <= 2) AS twoTrueWithNull")

    result.toList.head should equal(Map(
      "noneTrue" -> false,
      "oneTrue" -> true,
      "twoTrue" -> false,
      "nullPredicate" -> null,
      "noneTrueWithNull" -> null,
      "oneTrueWithNull" -> null,
      "twoTrueWithNull" -> false))
  }

  test("should single predicate on relationships") {
    val n1 = createLabeledNode(Map("x" -> 1), "Label")
    val n2 = createLabeledNode(Map("x" -> 2), "Label")
    val n3 = createLabeledNode(Map("x" -> 3), "Label")
    val n4 = createLabeledNode(Map("x" -> 4), "Label")
    relate(n1, n2, "T", Map("x" -> 1))
    relate(n2, n3, "T", Map("x" -> 2))
    relate(n3, n4, "T", Map("x" -> 3))
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query =
        "MATCH p=(n1:Label {x:1})-[*3]-(n4:Label {x:4}) " +
          "WHERE single(r IN relationships(p) WHERE r.x = 1) " +
          "RETURN " +
          " single(r IN relationships(p) WHERE r.x = 0) AS noneTrue," +
          " single(r IN relationships(p) WHERE r.x = 1) AS oneTrue, " +
          " single(r IN relationships(p) WHERE r.x <= 2) AS twoTrue, " +
          " single(r IN relationships(p) WHERE null) AS nullPredicate," +
          " single(r IN relationships(p) + [null] WHERE r.x = 0) AS noneTrueWithNull," +
          " single(r IN relationships(p) + [null] WHERE r.x = 1) AS oneTrueWithNull," +
          " single(r IN relationships(p) + [null] WHERE r.x <= 2) AS twoTrueWithNull")

    result.toList.head should equal(Map(
      "noneTrue" -> false,
      "oneTrue" -> true,
      "twoTrue" -> false,
      "nullPredicate" -> null,
      "noneTrueWithNull" -> null,
      "oneTrueWithNull" -> null,
      "twoTrueWithNull" -> false))
  }

  test("should handle nested reduce") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      query = "RETURN reduce(acc=0, s IN [reduce(acc=3, s IN [1,-1,1] | acc + s), 3] | acc + s) AS result")

    result.toList should equal(List(Map("result" -> 7)))
  }

  test("should handle deeply nested list-comprehensions") {
    def rec(count: Int): String =
      count match {
        case 0 => "[0]"
        case _ =>
          val i = "i" + count
          val inner = rec(count - 1)
          s"[$i IN [1] | $i + $inner[0]]"
      }

    val query = "RETURN "+rec(10)+" AS x"
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.toList should be(List(Map("x" -> List(10))))
  }

  test("should handle ANY (...) AND ANY (..)") {
    //given
    executeSingle(
      """create (n:Test {id: 1, ne: ["TIT", "O"]})
        |create (n2:Test {id: 2, ne: ["PER"]})
        |create (n)-[:NEXT]->(n2)
        |""".stripMargin)

    //when
    val result =
      executeWith(Configs.InterpretedAndSlottedAndPipelined,
        """MATCH path=(src:Test)-[r1:NEXT]->(dst:Test)
          |WHERE ANY(ne IN src.ne WHERE ne IN ["TIT"]) AND ANY(ne IN dst.ne WHERE ne IN ["PER"])
          |RETURN path""".stripMargin)

    //then
    result shouldNot be(empty)
  }

  test("list expression on top of nullable") {
    //given
    createLabeledNode("A")
    createLabeledNode("B")

    //when
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """MATCH (a:A)
        |MATCH (b:B)
        |OPTIONAL MATCH p=(a)-->(b)
        |RETURN [r IN relationships(p) WHERE type(r)='T' | r] AS result""".stripMargin)

    //then
    result.toList should equal(List(Map("result" -> null)))
  }
}
