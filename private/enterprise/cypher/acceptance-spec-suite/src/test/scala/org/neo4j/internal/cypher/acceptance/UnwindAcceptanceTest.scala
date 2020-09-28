/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.time.LocalDate
import java.util

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.internal.helpers.collection.MapUtil

class UnwindAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should unwind scalar integer") {
    val query = "UNWIND 7 AS x RETURN x"
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList should equal(List(Map("x" -> 7)))
  }

  test("should unwind scalar string") {
    val query = "UNWIND 'this string' AS x RETURN x"
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList should equal(List(Map("x" -> "this string")))
  }

  test("should unwind scalar boolean") {
    val query = "UNWIND false AS x RETURN x"
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList should equal(List(Map("x" -> false)))
  }

  test("should unwind scalar temporal") {
    val query = "UNWIND date('2019-05-07') AS x RETURN x"
    val result = executeWith(Configs.UDF, query)

    result.toList should equal(List(Map("x" -> LocalDate.of(2019, 5, 7))))
  }

  test("should unwind parameterized scalar value") {
    val query = "UNWIND $value AS x RETURN x"
    val result = executeWith(Configs.All, query, params = Map("value" -> 42))

    result.toList should equal(List(Map("x" -> 42)))

  }

  test("should unwind nodes") {
    val n = createNode("prop" -> 42)

    graph.withTx( tx => {
      val query = "UNWIND $nodes AS n WITH n WHERE n.prop = 42 RETURN n"
      val result = makeRewinadable(tx.execute(query, MapUtil.map("nodes", util.Arrays.asList(n))))
      result.toList should equal(List(Map("n" -> n)))
    })

  }

  test("should unwind nodes from literal list") {
    val nId = createNode("prop" -> 42).getId

    graph.withTx( tx => {
      val n = tx.getNodeById(nId)
      val query = "UNWIND [$node] AS n WITH n WHERE n.prop = 42 RETURN n"
      val result = makeRewinadable(tx.execute(query, MapUtil.map("node", n)))
      result.toList should equal(List(Map("n" -> n)))
    } )
  }

  test("should unwind scalar node") {

    val n = createNode("prop" -> 42)

    val query =
      """
        | MATCH (n {prop: 42})
        | WITH n
        | UNWIND n AS x
        | RETURN x
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.toList should equal(List(Map("x" -> n)))
  }

  test("should unwind relationships") {
    val a = createNode()
    val b = createNode()
    val rId = relate(a, b, "prop" -> 42).getId

    graph.withTx( tx => {
      val r = tx.getRelationshipById(rId)
      val query = "UNWIND $relationships AS r WITH r WHERE r.prop = 42 RETURN r"
      val result = makeRewinadable(tx.execute(query, MapUtil.map("relationships", util.Arrays.asList(r))))
      result.toList should equal(List(Map("r" -> r)))
    } )
  }

  test("should unwind relationships from literal list") {
    val a = createNode()
    val b = createNode()
    val rId = relate(a, b, "prop" -> 42).getId

    graph.withTx( tx => {
      val r = tx.getRelationshipById(rId)
      val query = "UNWIND [$relationship] AS r WITH r WHERE r.prop = 42 RETURN r"
      val result = makeRewinadable(tx.execute(query, MapUtil.map("relationship", r)))
      result.toList should equal(List(Map("r" -> r)))
    } )
  }

  test("should unwind scalar relationship") {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b)

    val query =
      """
        | MATCH ()-[r]->()
        | WITH r
        | UNWIND r AS x
        | RETURN x
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.toList should equal(List(Map("x" -> r)))
  }

  test("Unwind query should plan successfully with data") {
    createLabeledNode(Map("prop" -> 42, "other" -> 43), "Elements")
    val result = executeSingle(
      """
        |explain CYPHER RUNTIME=slotted MATCH (m:Elements:Creation:Model{polyglotID:$polyglotID})
        |WITH *
        |UNWIND keys($relationships) as relationType
        |UNWIND $relationships[relationType] as relation
        |MATCH (a:Elements:`val1`:Artefact:ModelItem{name:relation.from}) <-[:contains]-(m)
        |MATCH (b:Elements:`val2`:Artefact:ModelItem{name:relation.to}) <-[:contains]-(m)
        |CALL dbms.procedures() YIELD name AS rel
        |RETURN *""".stripMargin
    )
  }

  test("changing the order of the match clauses should not impact the result") {
    // setup
    execute(
      """UNWIND [{prop:["foo", "bar", "baz", "blub"], properties:{eid:"AAA"}}] AS row
        |CREATE (n:SmallMol{prop: row.prop})
        |SET n += row.properties
        |SET n:Drug;""".stripMargin)
    execute(
      """UNWIND [{eid:"CCC", properties:{}}] AS row
        |CREATE (n:Protein{eid: row.eid}) SET n += row.properties;""".stripMargin)

    val order1 = executeWith(Configs.All,
      """PROFILE UNWIND [{start: {eid:"CCC"}, end: {prop:["foo", "bar", "baz", "blub"]}, properties:{eid:"BBB"}}] AS row
        |MATCH (end:SmallMol{prop: row.end.prop})
        |MATCH (start:Protein{eid: row.start.eid})
        |RETURN *""".stripMargin
    )

    val description = order1.executionPlanDescription()
    description should includeSomewhere.aPlan("CartesianProduct")
    description should not(includeSomewhere.aPlan("ValueHashJoin"))
    val res1: Seq[Map[String, Any]] = order1.toComparableResult
    res1.size should be(1)
    val order2 = executeWith(Configs.All,
      """PROFILE UNWIND [{start: {eid:"CCC"}, end: {prop:["foo", "bar", "baz", "blub"]}, properties:{eid:"BBB"}}] AS row
         MATCH (start:Protein{eid: row.start.eid})
         MATCH (end:SmallMol{prop: row.end.prop})
         RETURN *""".stripMargin
    )

    order2.executionPlanDescription() should includeSomewhere.aPlan("CartesianProduct")
    order2.executionPlanDescription() should not(includeSomewhere.aPlan("ValueHashJoin"))
    val res2 = order2.toComparableResult
    res1 should equal(res2)
  }

  test("should not support aggregation in a list to unwind") {
    createLabeledNode("Person")

    failWithError(Configs.All,
      "MATCH (p:Person) UNWIND ['j', count(*)] AS var RETURN var",
     "Can't use aggregating expressions inside of expressions executing over lists"
    )
  }

  test("should support aggregation in unwind horizon") {
    createLabeledNode("Person")

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (p:Person) UNWIND ['a', 'b', 'c' ] AS var WITH var, count(*) as c RETURN var,c"
    )
    result.toSet shouldBe Set(Map("var" -> "a", "c" -> 1), Map("var" -> "b", "c" -> 1), Map("var" -> "c", "c" -> 1))
  }
}
