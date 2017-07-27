/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, QueryStatisticsTestSupport}

class StartAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("START n=node:index(key = \"value\") RETURN n") {
    val node = createNode()
    graph.inTx {
      graph.index().forNodes("index").add(node, "key", "value")
    }

    val result = executeWithAllPlannersAndCompatibilityMode("""START n=node:index(key = "value") RETURN n""").toList

    result should equal(List(Map("n"-> node)))
  }

  test("START n=node:index(\"key:value\") RETURN n") {
    val node = createNode()
    graph.inTx {
      graph.index().forNodes("index").add(node, "key", "value")
    }

    val result = executeWithAllPlannersAndCompatibilityMode("""START n=node:index("key:value") RETURN n""").toList

    result should equal(List(Map("n"-> node)))
  }

  test("start + where") {
    val node = createNode(Map("prop" -> 42))
    val otherNode = createNode(Map("prop" -> 21))

    graph.inTx {
      graph.index().forNodes("index").add(node, "key", "value")
      graph.index().forNodes("index").add(otherNode, "key", "value")
    }

    val result = executeWithAllPlannersAndCompatibilityMode("""START n=node:index("key:value") WHERE n.prop = 42 RETURN n""").toList

    result should equal(List(Map("n"-> node)))
  }

  test("Relationship legacy index") {
    val node = createNode(Map("prop" -> 42))
    val otherNode = createNode(Map("prop" -> 21))
    val relationship = relate(node, otherNode)

    graph.inTx {
      val relationshipIndex = graph.index().forRelationships("relIndex")
      relationshipIndex.add(relationship, "key", "value")
    }

    val query = "START r=relationship:relIndex('key:*') RETURN r"
    val result = executeWithAllPlannersAndCompatibilityMode(query)

    result.toList should equal(List(Map("r"-> relationship)))
  }

  test("START n=node(0) RETURN n") {
    val node = createNode()
    createNode()
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("START n=node(0) RETURN n").toList

    result should equal(List(Map("n"-> node)))
  }

  test("START n=node({id}) RETURN n") {
    val node = createNode()
    createNode()
    val result = executeWithAllPlannersAndCompatibilityMode("START n=node({id}) RETURN n", "id" -> node.getId).toList

    result should equal(List(Map("n"-> node)))
  }

  test("START n=node(0,1) RETURN n") {
    val node1 = createNode()
    val node2 = createNode()
    val result = executeWithAllPlannersAndCompatibilityMode("START n=node(0,1) RETURN n").toList

    result should equal(List(Map("n" -> node1), Map("n" -> node2)))
  }

  test("START n=node({id}) RETURN n, id-> [0,1]") {

    val node1 = createNode()
    val node2 = createNode()
    val result = executeWithAllPlannersAndCompatibilityMode("START n=node({id}) RETURN n",
                                                            "id" -> List(node1.getId, node2.getId)).toList

    result should equal(List(Map("n" -> node1), Map("n" -> node2)))
  }

  test("START n=node(0),m=node(1) RETURN n") {
    val node1 = createNode()
    val node2 = createNode()
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("START n=node(0),m=node(1) RETURN n,m").toList

    result should equal(List(Map("n"-> node1, "m" -> node2)))
  }

  test("START n=node({id1}),m=node({id2}) RETURN n") {
    val node1 = createNode()
    val node2 = createNode()
    val result = executeWithAllPlannersAndCompatibilityMode("START n=node({id1}),m=node({id2}) RETURN n,m",
                                                                       "id1" -> node1.getId, "id2" -> node2.getId).toList

    result should equal(List(Map("n"-> node1, "m" -> node2)))
  }

  test("START n=node(*) RETURN n") {
    val node1 = createNode()
    val node2 = createNode()
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("START n=node(*) RETURN n").toList

    result should equal(List(Map("n"-> node1), Map("n" -> node2)))
  }

  test("START r=rel(0) RETURN r") {
    val rel = relate(createNode(), createNode()).getId
    val result = executeWithAllPlannersAndCompatibilityMode("START r=rel(0) RETURN id(r)").toList

    result should equal(List(Map("id(r)"-> rel)))
  }

  test("START r=rel({id}) RETURN r") {
    val rel = relate(createNode(), createNode()).getId
    relate(createNode(), createNode()).getId
    val result = executeWithAllPlannersAndCompatibilityMode("START r=rel({id}) RETURN id(r)", "id" -> rel).toList

    result should equal(List(Map("id(r)"-> rel)))
  }

  test("START r=rel(0,1) RETURN r") {
    val rel1 = relate(createNode(), createNode()).getId
    val rel2 = relate(createNode(), createNode()).getId
    val result = executeWithAllPlannersAndCompatibilityMode("START r=rel(0,1) RETURN id(r)").toList

    result should equal(List(Map("id(r)"-> rel1),Map("id(r)"-> rel2)))
  }

  test("START r=rel({id}) RETURN r, id -> [0,1]") {
    val rel1 = relate(createNode(), createNode()).getId
    val rel2 = relate(createNode(), createNode()).getId
    val result = executeWithAllPlannersAndCompatibilityMode("START r=rel({id}) RETURN id(r)", "id" -> List(rel1,rel2)).toList

    result should equal(List(Map("id(r)"-> rel1),Map("id(r)"-> rel2)))
  }

  test("START r=rel(0),rr=rel(1) RETURN r") {
    val rel1 = relate(createNode(), createNode()).getId
    val rel2 = relate(createNode(), createNode()).getId
    val result = executeWithAllPlannersAndCompatibilityMode("START r=rel(0),rr=rel(1) RETURN id(r), id(rr)").toList

    result should equal(List(Map("id(r)"-> rel1, "id(rr)"-> rel2)))
  }

  test("START r=rel({id1}),rr=rel({id2}) RETURN r") {
    val rel1 = relate(createNode(), createNode()).getId
    val rel2 = relate(createNode(), createNode()).getId
    val result = executeWithAllPlannersAndCompatibilityMode("START r=rel({id1}),rr=rel({id2}) RETURN id(r), id(rr)",
                                                            "id1" -> rel1, "id2" -> rel2).toList

    result should equal(List(Map("id(r)"-> rel1, "id(rr)"-> rel2)))
  }

  test("START r=rel(*) RETURN r") {
    val rel1 = relate(createNode(), createNode()).getId
    val rel2 = relate(createNode(), createNode()).getId
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("START r=rel(*) RETURN id(r)").toList

    result should equal(List(Map("id(r)"-> rel1),Map("id(r)"-> rel2)))
  }

  test("Relationship legacy index mk II") {
    val node = createNode(Map("prop" -> 42))
    val otherNode = createNode(Map("prop" -> 21))
    val relationship = relate(node, otherNode)

    graph.inTx {
      val relationshipIndex = graph.index().forRelationships("relIndex")
      relationshipIndex.add(relationship, "key", "value")
    }

    val query = "START r=relationship:relIndex('key:*') MATCH (a)-[r]-(b) RETURN r"
    val result = executeWithAllPlannersAndCompatibilityMode(query)

    result.toList should equal(List(
      Map("r"-> relationship),
      Map("r"-> relationship)
    ))
  }

  test("Relationship legacy index mk III") {
    val node = createNode(Map("prop" -> 42))
    val otherNode = createNode(Map("prop" -> 21))
    val relationship = relate(node, otherNode)

    graph.inTx {
      val relationshipIndex = graph.index().forRelationships("relIndex")
      relationshipIndex.add(relationship, "key", "value")
    }

    val query = "START r=relationship:relIndex('key:*') MATCH (a)-[r]->(b) RETURN r"
    val result = executeWithAllPlannersAndCompatibilityMode(query)

    result.toList should equal(List(
      Map("r"-> relationship)
    ))
  }
  test("Should return unique relationship on rel-id") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val ab = relate(a, b)
    val ac = relate(a, c)
    val result = executeWithAllPlannersAndCompatibilityMode(
      """start a=node(0), ab=relationship(0)
        |match (a)-[ab]->(b)
        |return b
      """.stripMargin)

    result.toList should equal(List(Map("b" -> b)))
  }

  test("Should return unique node on node-id") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val ab = relate(a, b)
    val ac = relate(a, c)
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode(
      """start a=node(0), b=node(1)
        |match (a)-[ab]->(b)
        |return b
      """.stripMargin)

    result.toList should equal(List(Map("b" -> b)))
  }

  test("Should return unique relationship on multiple rel-id") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()
    val e = createNode()
    val ab = relate(a, b)
    val ac = relate(a, c)
    val bd = relate(b, d)
    val ce = relate(c, e)
    val result = executeWithAllPlannersAndCompatibilityMode(
      """start a=node(0), ab=relationship(0), bd=relationship(2)
        |match (a)-[ab]->(b)-[bd]->(d)
        |return b, d
      """.stripMargin)

    result.toList should equal(List(Map("b" -> b, "d" -> d)))
  }

  test("Should return unique relationship on rel-ids") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()
    val ab = relate(a, b)
    val ac = relate(a, c)
    val ad = relate(a, d)
    val result = executeWithAllPlannersAndCompatibilityMode(
      """start a=node(0), ab=relationship(0, 1)
        |match (a)-[ab]->(b)
        |return b
      """.stripMargin)

    result.toSet should equal(Set(Map("b" -> c), Map("b" -> b)))
  }

  test("should return correct results on combined node and relationship index starts") {
    val node = createNode()
    val resultNode = createNode()
    val rel = relate(node, resultNode)
    relate(node, createNode())

    graph.inTx {
      graph.index.forNodes("nodes").add(node, "key", "A")
      graph.index.forRelationships("rels").add(rel, "key", "B")
    }

    val result = innerExecute("START n=node:nodes(key = 'A'), r=rel:rels(key = 'B') MATCH (n)-[r]->(b) RETURN b")
    result.toList should equal(List(Map("b" -> resultNode)))
  }

}
