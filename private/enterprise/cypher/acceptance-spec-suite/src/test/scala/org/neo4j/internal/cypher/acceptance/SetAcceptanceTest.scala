/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class SetAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  test("optional match and set") {
    val n1 = createLabeledNode("L1")
    val n2 = createLabeledNode("L2")
    relate(n1, n2, "R1")

    // only fails when returning distinct...
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |MATCH (n1:L1)-[:R1]->(n2:L2)
        |OPTIONAL MATCH (n3)<-[r:R2]-(n2)
        |SET n3.prop = false
        |RETURN distinct n2
      """.stripMargin)

    result.toList should be(List(Map("n2" -> n2)))
  }

  test("should be able to force a type change of a node property") {
    // given
    createNode("prop" -> 1337)

    // when
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n) SET n.prop = tofloat(n.prop)")

    executeWith(Configs.All, "MATCH (n) RETURN n.prop").head("n.prop") shouldBe a[java.lang.Double]
  }

  test("should be able to force a type change of a relationship property") {
    // given
    relate(createNode(), createNode(), "prop" -> 1337)

    // when
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH ()-[r]->() SET r.prop = tofloat(r.prop)")

    executeWith(Configs.All, "MATCH ()-[r]->() RETURN r.prop").head("r.prop") shouldBe a[java.lang.Double]
  }

  test("should be able to set property to collection") {
    // given
    val node = createNode()

    // when
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n) SET n.property = ['foo','bar'] RETURN n.property")

    // then
    assertStats(result, propertiesWritten = 1)
    node should haveProperty("property")

    // and
    val result2 = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n) WHERE n.property = ['foo','bar'] RETURN count(*)")
    result2.columnAs("count(*)").toList should be(List(1))
  }

  test("should not be able to set property to collection of collections") {
    // given
    createNode()

    // when
    failWithError(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n) SET n.property = [['foo'],['bar']] RETURN n.property",
      "Collections containing collections can not be stored in properties.")
  }

  test("should not be able to set property to collection with null value") {
    // given
    createNode()

    // when
    failWithError(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n) SET n.property = [null,null] RETURN n.property",
      "Collections containing null values can not be stored in properties.")

  }

  test("should add properties on a mixed list of nodes and properties") {
    relate(createNode(Map("data" -> 1)), createNode(), "data" -> 2)

    val q =
      """
        |MATCH (n)-[r]->()
        |UNWIND [n, r] AS entity
        |SET entity += {
        |  prop: entity.data
        |}
        |RETURN entity.prop, entity.data
        |""".stripMargin

    val r = executeWith(Configs.InterpretedAndSlottedAndPipelined, q)
    r.toList should equal(List(Map("entity.prop" -> 1, "entity.data" -> 1), Map("entity.prop" -> 2, "entity.data" -> 2)))
  }

  test("should set properties on a mixed list of nodes and properties") {
    relate(createNode(Map("data" -> 1)), createNode(), "data" -> 2)

    val q =
      """
        |MATCH (n)-[r]->()
        |UNWIND [n, r] AS entity
        |SET entity = {
        |  prop: entity.data
        |}
        |RETURN entity.prop, entity.data
        |""".stripMargin

    val r = executeWith(Configs.InterpretedAndSlottedAndPipelined, q)
    r.toList should equal(List(Map("entity.prop" -> 1, "entity.data" -> null), Map("entity.prop" -> 2, "entity.data" -> null)))
  }

  //Not suitable for the TCK
  test("set a property by selecting the node through an expression") {
    // given
    val a = createNode()

    // when
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n) SET (CASE WHEN true THEN n END).name = 'neo4j' RETURN count(*)")

    // then
    assertStats(result, propertiesWritten = 1)
    a should haveProperty("name").withValue("neo4j")
  }

  //Not suitable for the TCK
  test("set a property by selecting the relationship through an expression") {
    // given
    val r = relate(createNode(), createNode())

    // when
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH ()-[r]->() SET (CASE WHEN true THEN r END).name = 'neo4j' RETURN count(*)")

    // then
    assertStats(result, propertiesWritten = 1)
    r should haveProperty("name").withValue("neo4j")
  }

  //Not suitable for the TCK
  test("should set properties on nodes with foreach and indexes") {
    val n1 = createNode()
    val n2 = createNode()
    val n3 = createNode()

    val query = "MATCH (n) WITH collect(n) AS nodes, $param AS data FOREACH (idx IN range(0,size(nodes)-1) | SET (nodes[idx]).num = data[idx])"
    val result = executeWith(Configs.InterpretedAndSlotted, query, params = Map("param" ->  Array("1", "2", "3")))

    assertStats(result, propertiesWritten = 3)
    n1 should haveProperty("num").withValue("1")
    n2 should haveProperty("num").withValue("2")
    n3 should haveProperty("num").withValue("3")
  }

  //Not suitable for the TCK
  test("should set properties on relationships with foreach and indexes") {
    val r1 = relate(createNode(), createNode())
    val r2 = relate(createNode(), createNode())
    val r3 = relate(createNode(), createNode())

    val query = "MATCH ()-[r]->() WITH collect(r) AS rels, $param as data FOREACH (idx IN range(0,size(rels)-1) | SET (rels[idx]).num = data[idx])"
    val result = executeWith(Configs.InterpretedAndSlotted, query, params = Map("param" ->  Array("1", "2", "3")))

    assertStats(result, propertiesWritten = 3)
    r1 should haveProperty("num").withValue("1")
    r2 should haveProperty("num").withValue("2")
    r3 should haveProperty("num").withValue("3")
  }

  //Not suitable for the TCK
  test("should fail at runtime when the expression is not a node or a relationship") {
    failWithError(Configs.InterpretedAndSlottedAndPipelined, "SET (CASE WHEN true THEN $node END).name = 'neo4j' RETURN count(*)",
      "Type mismatch: expected Map, Node, Relationship, Point, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was Integer",
      params = Map("node" -> 42))
  }

  //Not suitable for the TCK
  test("mark nodes in path") {
    // given
    val a = createNode()
    val b = createNode()
    val c = createNode()
    relate(a, b)
    relate(b, c)

    // when
    val q = "MATCH p=(a)-->(b)-->(c) WHERE id(a) = 0 AND id(c) = 2 WITH p FOREACH(n IN nodes(p) | SET n.marked = true)"

    executeWith(Configs.InterpretedAndSlotted, q)

    // then
    a should haveProperty("marked").withValue(true)
    b should haveProperty("marked").withValue(true)
    c should haveProperty("marked").withValue(true)
  }

  //Not suitable for the TCK
  test("set += works well inside foreach") {
    // given
    val a = createNode("a"->"A")
    val b = createNode("b"->"B")
    val c = createNode("c"->"C")

    // when
    executeWith(Configs.InterpretedAndSlotted, "MATCH (n) WITH collect(n) AS nodes FOREACH(x IN nodes | SET x += {x:'X'})")

    // then
    a should haveProperty("a").withValue("A")
    b should haveProperty("b").withValue("B")
    c should haveProperty("c").withValue("C")
    a should haveProperty("x").withValue("X")
    b should haveProperty("x").withValue("X")
    c should haveProperty("x").withValue("X")
  }

  //Not suitable for the TCK
  test("set = works well inside foreach") {
    // given
    val a = createNode("a"->"A")
    val b = createNode("b"->"B")
    val c = createNode("c"->"C")

    // when
    executeWith(Configs.InterpretedAndSlotted, "MATCH (n) WITH collect(n) as nodes FOREACH(x IN nodes | SET x = {a:'D', x:'X'})")

    // then
    a should haveProperty("a").withValue("D")
    b should haveProperty("a").withValue("D")
    c should haveProperty("a").withValue("D")
    b should not(haveProperty("b"))
    c should not(haveProperty("c"))
    a should haveProperty("x").withValue("X")
    b should haveProperty("x").withValue("X")
    c should haveProperty("x").withValue("X")
  }

  test("set works on chained properties") {
    val n = createNode("a" -> 123)

    executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n) WITH {node: n} AS map SET map.node.b = 'hello'")

    n should haveProperty("a").withValue(123)
    n should haveProperty("b").withValue("hello")
  }

  //Not suitable for the TCK
  test("Lost updates should not happen on set node property") {
    val init: () => Unit = () => createNode("prop" -> 0)
    val query = "MATCH (n) SET n.prop = n.prop + 1"
    val resultQuery = "MATCH (n) RETURN n.prop"
    testLostUpdates(init, query, resultQuery, 10, 10)
  }

  //Not suitable for the TCK
  test("Lost updates should not happen on set node property with an entangled expression") {
    val init: () => Unit = () => createNode("prop" -> 0)
    val query = "MATCH (n) SET n.prop = 2 + (10 * n.prop) / 10 - 1"
    val resultQuery = "MATCH (n) RETURN n.prop"
    testLostUpdates(init, query, resultQuery, 10, 10)
  }

  //Not suitable for the TCK
  test("Lost updates should not happen for set node property with map") {
    val init: () => Unit = () => createNode("prop" -> 0)
    val query = "MATCH (n) SET n = {prop: n.prop + 1}"
    val resultQuery = "MATCH (n) RETURN n.prop"
    testLostUpdates(init, query, resultQuery, 10, 10)
  }

  //Not suitable for the TCK
  test("Lost updates should not happen on set relationship property") {
    val init: () => Unit = () => relate(createNode(), createNode(), "prop" -> 0)
    val query = "MATCH ()-[r]->() SET r.prop = r.prop + 1"
    val resultQuery = "MATCH ()-[r]->() RETURN r.prop"
    testLostUpdates(init, query, resultQuery, 10, 10)
  }

  //Not suitable for the TCK
  test("Lost updates should not happen on set relationship property with an entangled expression") {
    val init: () => Unit = () => relate(createNode(), createNode(), "prop" -> 0)
    val query = "MATCH ()-[r]->() SET r.prop = 2 + (10 * r.prop) / 10 - 1"
    val resultQuery = "MATCH ()-[r]->() RETURN r.prop"
    testLostUpdates(init, query, resultQuery, 10, 10)
  }

  //Not suitable for the TCK
  test("Lost updates should not happen for set relationship property with map") {
    val init: () => Unit = () => relate(createNode(), createNode(), "prop" -> 0)
    val query = "MATCH ()-[r]->() SET r = {prop: r.prop + 1}"
    val resultQuery = "MATCH ()-[r]->() RETURN r.prop"
    testLostUpdates(init, query, resultQuery, 10, 10)
  }

  // Lost updates are still an issue, and it's hard to identify some cases.
  // We try to document some typical cases below

  // We do not support this because, even though the test is just a simple case,
  // in general we would have to solve a complex data flow equation in order
  // to solve this without being too conservative and do unnecessary exclusive locking
  // (which could be really bad for concurrency in bad cases)
  //Not suitable for the TCK
  ignore("Lost updates should not happen on set node property with the read in a preceding statement") {
    val init: () => Unit = () => createNode("prop" -> 0)
    val query = "MATCH (n) WITH n.prop as p SET n.prop = p + 1"
    val resultQuery = "MATCH (n) RETURN n.prop"
    testLostUpdates(init, query, resultQuery, 10, 10)
  }

  //Not suitable for the TCK
  ignore("lost updates should not happen on set node properties from map with circular dependencies") {
    val init: () => Unit = () => createNode("prop" -> 0, "prop2" -> 0)
    val query = "match (n) set n += { prop: n.prop2 + 1, prop2: n.prop + 1 }"
    val resultQuery = "MATCH (n) RETURN n.prop + n.prop2"
    testLostUpdates(init, query, resultQuery, 10, 20)
  }

  private def testLostUpdates(init: () => Unit,
                              query: String,
                              resultQuery: String,
                              updates: Int,
                              resultValue: Int) = {
    init()
    val threads = (0 until updates).map { _ =>
      new Thread(new Runnable {
        override def run(): Unit = {
          execute(query)
        }
      })
    }
    threads.foreach(_.start())
    threads.foreach(_.join())

    val result = executeScalar[Long](resultQuery)
    assert(result == resultValue, s": we lost updates!")

    // Reset for run on next planner
    execute("MATCH (n) DETACH DELETE n")
  }
}
