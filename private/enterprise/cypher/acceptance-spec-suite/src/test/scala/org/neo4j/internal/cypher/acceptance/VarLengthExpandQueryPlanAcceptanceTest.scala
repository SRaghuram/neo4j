/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.Path
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{ComparePlansWithAssertion, Configs, CypherComparisonSupport}

class VarLengthExpandQueryPlanAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("Plan should have right relationship direction") {
    setUp("From")
    val query = "MATCH (a:From {name:'Keanu Reeves'})-[*..4]->(e:To {name:'Andres'}) RETURN *"

    executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)").containingArgument("(e)<-[:*..4]-(a)")
        plan should includeSomewhere.aPlan("NodeByLabelScan").containingArgument(":To")
      }))
  }

  test("Plan should have right relationship direction, other direction") {
    setUp("To")
    val query = "PROFILE MATCH (a:From {name:'Keanu Reeves'})-[*..4]->(e:To {name:'Andres'}) RETURN *"
    executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)").containingArgument("(a)-[:*..4]->(e)")
        plan should includeSomewhere.aPlan("NodeByLabelScan").containingArgument(":From")
      }))
  }

  test("Plan pruning var expand on distinct var-length match") {
    val query = "MATCH (a)-[*1..2]->(c) RETURN DISTINCT c"
    executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(Pruning)")
      }))
  }

  test("Plan pruning var expand on distinct var-length match with projection and aggregation") {
    val query = "MATCH (a)-[*1..2]->(c) WITH DISTINCT c RETURN count(*)"
    executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(Pruning)")
      }))
  }

  test("query with distinct aggregation") {
    val query = "MATCH (from)-[*1..3]->(to) RETURN count(DISTINCT to)"
    executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(Pruning)")
      }))
  }

  test("Simple query that filters between expand and distinct") {
    val query = "MATCH (a)-[*1..3]->(b:X) RETURN DISTINCT b"
    executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(Pruning)")
      }))
  }

  test("Query that aggregates before making the result DISTINCT") {
    val query = "MATCH (a)-[:R*1..3]->(b) WITH count(*) AS count RETURN DISTINCT count"
    executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)")
      }))
  }

  test("Double var expand with distinct result") {
    val query = "MATCH (a)-[:R*1..3]->(b)-[:T*1..3]->(c) RETURN DISTINCT c"
    executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(Pruning)")
      }))
  }

  test("var expand followed by normal expand") {
    val query = "MATCH (a)-[:R*1..3]->(b)-[:T]->(c) RETURN DISTINCT c"
    executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(Pruning)")
      }))
  }

  test("optional match can be solved with PruningVarExpand") {
    val query = "MATCH (a) OPTIONAL MATCH (a)-[:R*1..3]->(b)-[:T]->(c) RETURN DISTINCT c"
    executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(Pruning)")
      }))
  }

  test("should not rewrite when doing non-distinct aggregation") {
    val query = "MATCH (a)-[*1..3]->(b) RETURN b, count(*)"
    executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)")
      }))
  }

  test("on longer var-lengths, we also use PruningVarExpand") {
    val query = "MATCH (a)-[*4..5]->(b) RETURN DISTINCT b"
    executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(Pruning)")
      }))
  }

  test("Do not plan pruning var expand for length=1") {
    val query = "MATCH (a)-[*1..1]->(b) RETURN DISTINCT b"
    executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)")
      }))
  }

  test("AllNodesInPath") {
    graph.execute("CREATE (a:A {foo: 'bar'})-[:REL]->(b:B {foo: 'bar'})-[:REL]->(c:C {foo: 'bar'})-[:REL]->(d:D {foo: 'bar', name: 'd'})")
    val query = """MATCH p = (pA)-[:REL*3..3]->(pB)
                  |WHERE all(i IN nodes(p) WHERE i.foo = 'bar')
                  |RETURN pB.name """.stripMargin
    val result = executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)")
      }))
    result.toList should equal(List(Map("pB.name" -> "d")))
  }

  test("AllRelationships") {
    graph.execute("CREATE (a:A)-[:REL {foo: 'bar'}]->(b:B)-[:REL {foo: 'bar'}]->(c:C)-[:REL {foo: 'bar'}]->(d:D {name: 'd'})")
    val query = """MATCH p = (pA)-[:REL*3..3  {foo:'bar'}]->(pB)
                  |WHERE all(i IN rels(p) WHERE i.foo = 'bar')
                  |RETURN pB.name """.stripMargin
    val result = executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)")
      }))
    result.toList should equal(List(Map("pB.name" -> "d")))
  }

  test("AllRelationshipsInPath") {
    graph.execute("CREATE (a:A)-[:REL {foo: 'bar'}]->(b:B)-[:REL {foo: 'bar'}]->(c:C)-[:REL {foo: 'bar'}]->(d:D {name: 'd'})")
    val query = """MATCH p = (pA)-[:REL*3..3]->(pB)
                  |WHERE all(i IN rels(p) WHERE i.foo = 'bar')
                  |RETURN pB.name """.stripMargin
    val result = executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)")
      }))
    result.toList should equal(List(Map("pB.name" -> "d")))
  }

  test("NoNodeInPath") {
    graph.execute("CREATE (a:A {foo: 'bar'})-[:REL]->(b:B {foo: 'bar'})-[:REL]->(c:C {foo: 'bar'})-[:REL]->(d:D {foo: 'bar', name: 'd'})")
    val query = """MATCH p = (pA)-[:REL*3..3]->(pB)
                  |WHERE none(i IN nodes(p) WHERE i.foo = 'barz')
                  |RETURN pB.name """.stripMargin
    val result = executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)")
      }))
    result.toList should equal(List(Map("pB.name" -> "d")))
  }

  test("NoRelationshipInPath") {
    graph.execute("CREATE (a:A)-[:REL {foo: 'bar'}]->(b:B)-[:REL {foo: 'bar'}]->(c:C)-[:REL {foo: 'bar'}]->(d:D {name: 'd'})")
    val query = """MATCH p = (pA)-[:REL*3..3]->(pB)
                  |WHERE none(i IN rels(p) WHERE i.foo = 'barz')
                  |RETURN pB.name """.stripMargin
    val result = executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)")
      }))
    result.toList should equal(List(Map("pB.name" -> "d")))
  }

  test("AllNodesInPath with inner predicate using labelled nodes of the path") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """ MATCH p = (:NODE)-[*1]->(:NODE)
        | WHERE ALL(x IN nodes(p) WHERE single(y IN nodes(p) WHERE y = x))
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("AllNodesInPath with inner predicate using labelled named nodes of the path") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """ MATCH p = (start:NODE)-[rel*1]->(end:NODE)
        | WHERE ALL(x IN nodes(p) WHERE single(y IN nodes(p) WHERE y = x))
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("AllNodesInPath with inner predicate using nodes of the path") {
    val node1 = createNode()
    val node2 = createNode()
    relate(node1,node2)

    val query =
      """
        | MATCH p = ()-[*1]->()
        | WHERE ALL(x IN nodes(p) WHERE single(y IN nodes(p) WHERE y = x))
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }


  test("AllNodesInPath with complex inner predicate using the start node and end node") {
    val node1 = createLabeledNode(Map("prop" -> 1), "NODE")
    val node2 = createLabeledNode(Map("prop" -> 1),"NODE")
    relate(node1,node2)

    val query =
      """
        | MATCH p = (start:NODE)-[*1..2]->(end:NODE)
        | WHERE ALL(x IN nodes(p) WHERE x.prop = nodes(p)[0].prop AND x.prop = nodes(p)[1].prop)
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("AllNodesInPath with simple inner predicate") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """ MATCH p = (:NODE)-[*1]->(:NODE)
        | WHERE ALL(x IN nodes(p) WHERE length(p) = 1)
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("AllNodesInPath with inner predicate only using start node") {
    val node1 = createLabeledNode(Map("prop" -> 5),"NODE")
    val node2 = createLabeledNode(Map("prop" -> 5),"NODE")
    relate(node1,node2)

    val query =
      """ MATCH p = (n)-[r*1]->()
        | WHERE ALL(x IN nodes(p) WHERE x.prop = n.prop)
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("AllRelationshipsInPath with inner predicate using rels of the path") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """
        | MATCH p = (:NODE)-[*1]->(:NODE)
        | WHERE ALL(x IN rels(p) WHERE single(y IN rels(p) WHERE y = x))
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("AllRelationshipsInPath with simple inner predicate") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """
        | MATCH p = (:NODE)-[*1]->(:NODE)
        | WHERE ALL(x IN rels(p) WHERE length(p) = 1)
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("NoNodesInPath with simple inner predicate") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """
        | MATCH p = (:NODE)-[*1..2]->(:NODE)
        | WHERE NONE(x IN nodes(p) WHERE length(p) = 2)
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("NoRelationshipsInPath with simple inner predicate") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """
        | MATCH p = (:NODE)-[*1..2]->(:NODE)
        | WHERE NONE(x IN rels(p) WHERE length(p) = 2)
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("Do not plan pruning var executeWithCostPlannerAndInterpretedRuntimeOnly when path is needed") {
    val query = "MATCH p=(from)-[r*0..1]->(to) WITH nodes(p) AS d RETURN DISTINCT d"
    executeWith(Configs.InterpretedAndSlotted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)")
      }))
  }

  private def setUp(startLabel: String) {
    val a = createLabeledNode(Map("name" -> "Keanu Reeves"), "From")
    val b = createLabeledNode(Map("name" -> "Craig"), "User")
    val c = createLabeledNode(Map("name" -> "Olivia"), "User")
    val d = createLabeledNode(Map("name" -> "Carrie"), "User")
    val e = createLabeledNode(Map("name" -> "Andres"), "To")
    // Ensure compiler prefers to start at low cardinality 'To' node
    Range(0, 100).foreach(i => createLabeledNode(Map("name" -> s"node $i"), startLabel))
    relate(a, b)
    relate(b, c)
    relate(c, d)
    relate(d, e)
  }
}
