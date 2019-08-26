/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{ComparePlansWithAssertion, Configs, CypherComparisonSupport}

class CachedPropertyAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should cache properties for nodes") {
    createNode(Map("foo" -> 1))
    createNode(Map("foo" -> 111))
    createNode(Map("foo" -> 112))
    createNode(Map("foo" -> 113))
    createNode(Map("foo" -> 114))
    val res = executeWith(Configs.CachedProperty,"PROFILE MATCH (n) WHERE n.foo > 10 RETURN n.foo",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.
        aPlan("Projection")
        .containingArgument("{n.foo : cache[n.foo]}")
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
    val res = executeWith(Configs.CachedProperty,"PROFILE MATCH ()-[r]->() WHERE r.foo > 10 RETURN r.foo",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.
        aPlan("Projection")
        .containingArgument("{r.foo : cache[r.foo]}")
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
    val res = executeWith(Configs.InterpretedAndSlotted + Configs.Compiled, q,
      planComparisonStrategy = ComparePlansWithAssertion(_  should includeSomewhere.
        aPlan("Projection")
        .containingArgument("{m.prop : cache[m.prop], x.prop : cache[x.prop]}")
        .withDBHits(0),
        expectPlansToFail = Configs.Compiled // compiled does not cache properties and will therefore have DB hits
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
    val res = executeWith(Configs.InterpretedAndSlotted, q,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.
        aPlan("Projection")
        .containingArgument("{m.prop : cache[m.prop], x.prop : cache[x.prop]}")
        // As long as aggregation deleted all cached properties, we cannot assert on getting 0 DB hits here)
      )
    )

    res.toList should contain theSameElementsAs List(
      Map("m.prop" -> 1, "x.prop" -> 3),
      Map("m.prop" -> 1, "x.prop" -> 4),
      Map("m.prop" -> 2, "x.prop" -> 3),
      Map("m.prop" -> 2, "x.prop" -> 4))
  }

  test("should cache a node property on existence check - if it exists") {
    var n1: Node = null
    var n2: Node = null
    n1 = createNode()
    n2 = createNode(Map("foo" -> 2))

    val res = executeWith(Configs.CachedProperty, "PROFILE MATCH (n) WHERE EXISTS(n.foo) RETURN n.foo",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.
        aPlan("Projection")
        .containingArgument("{n.foo : cache[n.foo]}")
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

    val res = executeWith(Configs.CachedProperty, "PROFILE MATCH ()-[r]->() WHERE EXISTS(r.foo) RETURN r.foo",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.
        aPlan("Projection")
        .containingArgument("{r.foo : cache[r.foo]}")
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

    val res = executeWith(Configs.CachedProperty, "MATCH (n) WHERE NOT EXISTS(n.foo) RETURN EXISTS(n.foo) AS x, n.foo",
      executeBefore = () => {
        n2.setProperty("foo", 2)
        n3.removeProperty("foo")
        graph.createNode()
        val node = graphOps.createNode()
        node.setProperty("foo", 5);
      })

    res.executionPlanDescription() should includeSomewhere.
      aPlan("Projection").containingArgument("{x : EXISTS(cache[n.foo]), n.foo : cache[n.foo]}").onTopOf(
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

    val res = executeWith(Configs.CachedProperty, "MATCH ()-[r]->() WHERE NOT EXISTS(r.foo) RETURN EXISTS(r.foo) AS x, r.foo",
      executeBefore = () => {
        r2.setProperty("foo", 2)
        r3.removeProperty("foo")
        graph.createNode().createRelationshipTo(graph.createNode(), REL)
        graph.createNode().createRelationshipTo(graph.createNode(), REL).setProperty("foo", 5)
      })

    res.executionPlanDescription() should includeSomewhere.
      aPlan("Projection").containingArgument("{x : EXISTS(cache[r.foo]), r.foo : cache[r.foo]}").onTopOf(
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

    val res = executeWith(Configs.CachedProperty, "MATCH (x) WHERE x.prop = 2 WITH x AS y MATCH (y)-->(z) WITH y, collect(z) AS ignore RETURN y.prop")

    res.executionPlanDescription() should includeSomewhere.
      aPlan("Projection").containingArgument("{y.prop : cache[y.prop]}")
      .onTopOf(aPlan("EagerAggregation")
        .onTopOf(aPlan("Expand(All)")
          .onTopOf(aPlan("Projection").containingArgument("{y : x}")
            .onTopOf(aPlan("Filter").containingArgument("cache[x.prop] = $`  AUTOINT0`")))))

    res.toList should contain theSameElementsAs List(Map("y.prop" -> 2))
  }

}
