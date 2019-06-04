/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}

class CachedPropertyAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should cache properties for nodes") {
    createNode(Map("foo" -> 1))
    createNode(Map("foo" -> 111))
    createNode(Map("foo" -> 112))
    createNode(Map("foo" -> 113))
    createNode(Map("foo" -> 114))
    val res = executeWith(Configs.InterpretedAndSlotted,"MATCH (n) WHERE n.foo > 10 RETURN n.foo")

    res.executionPlanDescription() should includeSomewhere.
      aPlan("Projection").containingArgument("{n.foo : cache[n.foo]}").onTopOf(
      aPlan("Filter").containingArgumentRegex("cache\\[n.foo\\] > .*".r)
    )
    res.toList should equal(List(Map("n.foo" -> 111), Map("n.foo" -> 112), Map("n.foo" -> 113), Map("n.foo" -> 114)))
  }

  test("should cache properties for relationships") {
    relate(createNode(), createNode(), "foo" -> 1)
    relate(createNode(), createNode(), "foo" -> 20)
    relate(createNode(), createNode(), "foo" -> 30)
    val res = executeWith(Configs.InterpretedAndSlotted,"MATCH ()-[r]->() WHERE r.foo > 10 RETURN r.foo")

    res.executionPlanDescription() should includeSomewhere.
      aPlan("Projection").containingArgument("{r.foo : cache[r.foo]}").onTopOf(
      aPlan("Filter").containingArgumentRegex("cache\\[r.foo\\] > .*".r)
    )
    res.toList should equal(List(Map("r.foo" -> 20), Map("r.foo" -> 30)))
  }

  test("should cache properties in the presence of byzantine renamings") {
    val n = createNode(Map("prop" -> 1))
    val m = createNode(Map("prop" -> 1))
    val q ="MATCH (n), (m) WHERE n.prop = m.prop WITH n AS m, m AS x RETURN m.prop, x.prop"
    val res = executeWith(Configs.InterpretedAndSlotted, q, params = Map("n" -> n, "m" -> m))

    res.executionPlanDescription() should includeSomewhere.
      aPlan("Projection").containingArgument("{m.prop : cache[n.prop], x.prop : cache[m.prop]}")

    res.toList should equal(List(
      Map("m.prop" -> 1, "x.prop" -> 1),
      Map("m.prop" -> 1, "x.prop" -> 1),
      Map("m.prop" -> 1, "x.prop" -> 1),
      Map("m.prop" -> 1, "x.prop" -> 1)))
  }

  test("cached property existence - nodes") {
    var n1: Node = null
    var n2: Node = null
    var n3: Node = null
    var n4: Node = null
    graph.inTx {
      n1 = createNode()
      n2 = createNode()
      n3 = createNode(Map("foo" -> 3))
      n4 = createNode(Map("foo" -> 4))
    }

    val res = executeWith(Configs.InterpretedAndSlotted, "MATCH (n) WHERE NOT EXISTS(n.foo) RETURN EXISTS(n.foo) AS x, n.foo",
      executeBefore = () => {
        n2.setProperty("foo", 2)
        n3.removeProperty("foo")
        createNode()
        createNode(Map("foo" -> 5))
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
    graph.inTx {
      r1 = relate(createNode(), createNode())
      r2 = relate(createNode(), createNode())
      r3 = relate(createNode(), createNode(), "foo" -> 1)
      r4 = relate(createNode(), createNode(), "foo" -> 4)
    }

    val res = executeWith(Configs.InterpretedAndSlotted, "MATCH ()-[r]->() WHERE NOT EXISTS(r.foo) RETURN EXISTS(r.foo) AS x, r.foo",
      executeBefore = () => {
        r2.setProperty("foo", 2)
        r3.removeProperty("foo")
        relate(createNode(), createNode())
        relate(createNode(), createNode(), "foo" -> 5)
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

}
