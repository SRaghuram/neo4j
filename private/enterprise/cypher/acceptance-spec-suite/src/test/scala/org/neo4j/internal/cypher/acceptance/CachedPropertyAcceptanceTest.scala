/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class CachedPropertyAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should cache properties for nodes") {
    createNode(Map("foo" -> 1))
    createNode(Map("foo" -> 111))
    createNode(Map("foo" -> 112))
    createNode(Map("foo" -> 113))
    createNode(Map("foo" -> 114))
    val res = executeWith(Configs.InterpretedAndSlottedAndPipelined,"PROFILE MATCH (n) WHERE n.foo > 10 RETURN n.foo",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.
        aPlan("Projection")
        .containingArgumentForProjection("`n.foo`" -> "cache[n.foo]")
        .withDBHits(0)
        .onTopOf(
          aPlan("Filter").containingArgumentRegex("cache\\[n.foo\\] > .*".r)
        )
      )
    )
    res.toList should equal(List(Map("n.foo" -> 111), Map("n.foo" -> 112), Map("n.foo" -> 113), Map("n.foo" -> 114)))
  }

  test("should cache properties for relationships") {
    relate(createNode(), createNode(), "foo" -> 1)
    relate(createNode(), createNode(), "foo" -> 20)
    relate(createNode(), createNode(), "foo" -> 30)
    val res = executeWith(Configs.InterpretedAndSlottedAndPipelined,"PROFILE MATCH ()-[r]->() WHERE r.foo > 10 RETURN r.foo",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.
        aPlan("Projection")
        .containingArgumentForProjection("`r.foo`" -> "cache[r.foo]")
        .withDBHits(0)
        .onTopOf(
          aPlan("Filter").containingArgumentRegex("cache\\[r.foo\\] > .*".r)
        )
      )
    )
    res.toList should equal(List(Map("r.foo" -> 20), Map("r.foo" -> 30)))
  }

  test("should cache properties in the presence of byzantine renamings") {
    createLabeledNode(Map("prop" -> 1), "N")
    createLabeledNode(Map("prop" -> 2), "N")
    createLabeledNode(Map("prop" -> 3), "M")
    createLabeledNode(Map("prop" -> 4), "M")
    val q ="PROFILE MATCH (n:N), (m:M) WHERE n.prop <> m.prop WITH n AS m, m AS x RETURN m.prop, x.prop"
    val res = executeWith(Configs.InterpretedAndSlottedAndPipelined, q,
      planComparisonStrategy = ComparePlansWithAssertion(_  should includeSomewhere.
        aPlan("Projection")
        .containingArgumentForProjection("`m.prop`" -> "cache[m.prop]", "`x.prop`" -> "cache[x.prop]")
        .withDBHits(0)
      )
    )

    res.toList should contain theSameElementsAs List(
      Map("m.prop" -> 1, "x.prop" -> 3),
      Map("m.prop" -> 1, "x.prop" -> 4),
      Map("m.prop" -> 2, "x.prop" -> 3),
      Map("m.prop" -> 2, "x.prop" -> 4))
  }

  test("should cache properties in the presence of renamings and aggregations") {
    createLabeledNode(Map("prop" -> 1), "N")
    createLabeledNode(Map("prop" -> 2), "N")
    createLabeledNode(Map("prop" -> 3), "M")
    createLabeledNode(Map("prop" -> 4), "M")
    val q ="PROFILE MATCH (n:N), (m:M) WHERE n.prop <> m.prop WITH n AS m, m AS x, sum(m.prop) AS whoCares RETURN m.prop, x.prop"
    val res = executeWith(Configs.InterpretedAndSlottedAndPipelined, q,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.
        aPlan("Projection")
        .containingArgumentForProjection("`m.prop`" -> "cache[m.prop]", "`x.prop`" -> "cache[x.prop]")
        // As long as aggregation deleted all cached properties, we cannot assert on getting 0 DB hits here)
      )
    )

    res.toList should contain theSameElementsAs List(
      Map("m.prop" -> 1, "x.prop" -> 3),
      Map("m.prop" -> 1, "x.prop" -> 4),
      Map("m.prop" -> 2, "x.prop" -> 3),
      Map("m.prop" -> 2, "x.prop" -> 4))
  }

  test("should cache a node property when used in SET clause") {
    createNode(Map("prop" -> 43))

    val res = executeWith(Configs.InterpretedAndSlottedAndPipelined, "PROFILE MATCH (n) WHERE n.prop > 42 SET n.anotherProp = n.prop RETURN n.anotherProp",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.
        aPlan("SetProperty")
        .containingArgument("n.anotherProp = cache[n.prop]")
        .onTopOf(
          aPlan("Filter").containingArgumentRegex("cache\\[n.prop\\] > .*".r)
        )
      )
    )

    res.toList should equal(List(
      Map("n.anotherProp" -> 43)
    ))
  }

  test("should cache a node property on existence check - if it exists") {
    var n1: Node = null
    var n2: Node = null
    n1 = createNode()
    n2 = createNode(Map("foo" -> 2))

    val res = executeWith(Configs.InterpretedAndSlottedAndPipelined, "PROFILE MATCH (n) WHERE EXISTS(n.foo) RETURN n.foo",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.
        aPlan("Projection")
        .containingArgumentForProjection("`n.foo`" -> "cache[n.foo]")
        .withDBHits(0)
        .onTopOf(
          aPlan("Filter").containingArgument("EXISTS(cache[n.foo])")
        )
      )
    )

    res.toList should equal(List(
      Map("n.foo" -> 2)
    ))
  }

  test("should cache a relationship property on existence check - if it exists") {
    var r1: Relationship = null
    var r2: Relationship = null
    r1 = relate(createNode(), createNode())
    r2 = relate(createNode(), createNode(), "foo" -> 1)

    val res = executeWith(Configs.InterpretedAndSlottedAndPipelined, "PROFILE MATCH ()-[r]->() WHERE EXISTS(r.foo) RETURN r.foo",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.
        aPlan("Projection")
        .containingArgumentForProjection("`r.foo`" -> "cache[r.foo]")
        .withDBHits(0)
        .onTopOf(
          aPlan("Filter").containingArgument("EXISTS(cache[r.foo])")
        )
      )
    )

    res.toList should equal(List(
      Map("r.foo" -> 1)
    ))
  }

  test("cached property existence - nodes") {
    var n1: Node = null
    var n2: Node = null
    var n3: Node = null
    var n4: Node = null
    n1 = createNode()
    n2 = createNode()
    n3 = createNode(Map("foo" -> 3))
    n4 = createNode(Map("foo" -> 4))

    val res = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n) WHERE NOT EXISTS(n.foo) RETURN EXISTS(n.foo) AS x, n.foo",
      executeBefore = tx => {
        tx.getNodeById(n2.getId).setProperty("foo", 2)
        tx.getNodeById(n3.getId).removeProperty("foo")
        tx.createNode()
        val node = tx.createNode()
        node.setProperty("foo", 5)
      })

    res.executionPlanDescription() should includeSomewhere.
      aPlan("Projection").containingArgument("EXISTS(cache[n.foo]) AS x, cache[n.foo] AS `n.foo`").onTopOf(
      aPlan("Filter").containingArgument("not EXISTS(cache[n.foo])")
    )
    res.toList should contain theSameElementsAs List(
      Map("n.foo" -> null, "x" -> false),
      Map("n.foo" -> null, "x" -> false),
      Map("n.foo" -> null, "x" -> false)
    )
  }

  test("cached property existence - relationships") {
    var r1: Relationship = null
    var r2: Relationship = null
    var r3: Relationship = null
    var r4: Relationship = null
    r1 = relate(createNode(), createNode())
    r2 = relate(createNode(), createNode())
    r3 = relate(createNode(), createNode(), "foo" -> 1)
    r4 = relate(createNode(), createNode(), "foo" -> 4)

    val res = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH ()-[r]->() WHERE NOT EXISTS(r.foo) RETURN EXISTS(r.foo) AS x, r.foo",
      executeBefore = tx => {
        tx.getRelationshipById(r2.getId).setProperty("foo", 2)
        tx.getRelationshipById(r3.getId).removeProperty("foo")
        tx.createNode().createRelationshipTo(tx.createNode(), REL)
        tx.createNode().createRelationshipTo(tx.createNode(), REL).setProperty("foo", 5)
      })

    res.executionPlanDescription() should includeSomewhere.
      aPlan("Projection").containingArgumentForProjection("x" -> "EXISTS(cache[r.foo])", "`r.foo`" -> "cache[r.foo]").onTopOf(
      aPlan("Filter").containingArgument("not EXISTS(cache[r.foo])")
    )
    res.toList should contain theSameElementsAs List(
      Map("r.foo" -> null, "x" -> false),
      Map("r.foo" -> null, "x" -> false),
      Map("r.foo" -> null, "x" -> false)
    )
  }

  test("should handle rename followed by aggregation") {
    relate(createNode("prop" -> 2), createNode("prop" -> 3))

    val res = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (x) WHERE x.prop = 2 WITH x AS y MATCH (y)-->(z) WITH y, collect(z) AS ignore RETURN y.prop")

    res.executionPlanDescription() should includeSomewhere.
      aPlan("Projection").containingArgumentForProjection("`y.prop`" -> "cache[y.prop]")
      .onTopOf(aPlan("EagerAggregation")
        .onTopOf(aPlan("Expand(All)")
          .onTopOf(aPlan("Projection").containingArgumentForProjection("y" -> "x")
            .onTopOf(aPlan("Filter").containingArgument("cache[x.prop] = $autoint_0")))))

    res.toList should contain theSameElementsAs List(Map("y.prop" -> 2))
  }

  test("should handle cached property on null entity") {
    val a = createLabeledNode("A")

    val query = """MATCH (a:A)
                  |OPTIONAL MATCH (a)-[r:R]->(b:B)
                  |WITH a, {
                  |  x: b,
                  |  y: b.prop,
                  |  z: b.prop
                  |} as m
                  |RETURN a, m""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.executionPlanDescription() should includeSomewhere.
      aPlan("Projection").containingArgumentForProjection("m" -> "{x: b, y: cache[b.prop], z: cache[b.prop]}")

    result.toList should equal(List(Map("a" -> a,
                                        "m" -> Map("x" -> null,
                                                   "y" -> null,
                                                   "z" -> null))))
  }

  test("should handle cached property on null entity passed through list") {
    val a = createLabeledNode("A")

    val query = """MATCH (a:A)
                  |OPTIONAL MATCH (a)-[r:R]->(b:B)
                  |WITH collect(b)[0] as c, a
                  |WITH a, {
                  |  x: c,
                  |  y: c.prop,
                  |  z: c.prop
                  |} as m
                  |RETURN a, m""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.executionPlanDescription() should includeSomewhere.
      aPlan("Projection").containingArgumentForProjection("m" -> "{x: c, y: cache[c.prop], z: cache[c.prop]}")

    result.toList should equal(List(Map("a" -> a,
                                        "m" -> Map("x" -> null,
                                                   "y" -> null,
                                                   "z" -> null))))
  }

  test("should handle cached property after unwind") {
    createNode("prop" -> 123)

    val query =
      """MATCH (n)
        |WITH collect(n) AS ns
        |UNWIND ns AS x
        |UNWIND range(1, 10) AS y
        |RETURN x.prop, y
        |""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    val expectedResult = Range.inclusive(1, 10).map(y => Map("x.prop" -> 123, "y" -> y)).toSet

    result.executionPlanDescription() should includeSomewhere.aPlan("CacheProperties")
    result.toSet should equal(expectedResult)
  }

  test("should handle caching renamed variable") {
    // Note, this test case builds on the assumption that cardinality will be lower after the aggregation which might change if we improve cardinality estimation.
    createLabeledNode(Map("prop" -> 123), "A")
    createLabeledNode("B")
    val query =
      """PROFILE
        |MATCH (n:A)
        |WITH n AS n2, count(n) AS count
        |MATCH (n2), (m:B)
        |WITH n2 AS n3
        |MATCH (n3)
        |RETURN n3.prop""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection").containingArgumentForProjection("`n3.prop`" -> "cache[n3.prop]").withDBHits(0)
    result.executionPlanDescription() should includeSomewhere
      .aPlan("CacheProperties").containingArgument("cache[n2.prop]").withDBHits(1)
    result.toList shouldBe List(Map("n3.prop" -> 123))
  }
}
