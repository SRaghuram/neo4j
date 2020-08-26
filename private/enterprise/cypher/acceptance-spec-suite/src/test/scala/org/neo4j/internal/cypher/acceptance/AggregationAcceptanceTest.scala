/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.exceptions.SyntaxException
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.values.storable.DurationValue

class AggregationAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should allow RETURN * after aggregation inside of expression") {
    val node = createNode()
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |MATCH (owner)
        |WITH HEAD(COLLECT(42)) AS sortValue, owner
        |RETURN *
        |""".stripMargin)
    result.toList should equal(List(Map("sortValue" -> 42, "owner" -> node)))
  }

  test("should allow map projection with property selector after aggregation inside of expression") {
    createNode(Map("prop" -> 123))
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |MATCH (n)
        |WITH HEAD(COLLECT(42)) AS x, n
        |RETURN n { .prop, res: x }
        |""".stripMargin)
    result.toList shouldBe List(Map("n" -> Map("prop" -> 123, "res" -> 42)))
  }

  test("should allow map projection with property selector after WITH *, aggregation()") {
    createNode(Map("prop" -> 123))
    createNode(Map("prop" -> 321))
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |MATCH (n)
        |WITH *, max(n.prop) AS x
        |RETURN n { .prop, max: x }
        |""".stripMargin)
    result.toList shouldBe List(
      Map("n" -> Map("prop" -> 123, "max" -> 123)),
      Map("n" -> Map("prop" -> 321, "max" -> 321))
    )
  }

  // Non-deterministic query -- needs TCK design
  test("should aggregate using as grouping key expressions using variables in scope and nothing else") {
    val userId = createLabeledNode(Map("userId" -> 11), "User")
    relate(userId, createNode(), "FRIEND", Map("propFive" -> 1))
    relate(userId, createNode(), "FRIEND", Map("propFive" -> 3))
    relate(createNode(), userId, "FRIEND", Map("propFive" -> 2))
    relate(createNode(), userId, "FRIEND", Map("propFive" -> 4))

    val query1 = """MATCH (user:User {userId: 11})-[friendship:FRIEND]-()
                   |WITH user, collect(friendship)[toInteger($param * count(friendship))] AS selectedFriendship
                   |RETURN id(selectedFriendship) AS friendshipId, selectedFriendship.propFive AS propertyValue""".stripMargin
    val query2 = """MATCH (user:User {userId: 11})-[friendship:FRIEND]-()
                   |WITH user, collect(friendship) AS friendships
                   |WITH user, friendships[toInteger($param * size(friendships))] AS selectedFriendship
                   |RETURN id(selectedFriendship) AS friendshipId, selectedFriendship.propFive AS propertyValue""".stripMargin
    val params = Map("param" -> 3)

    val result1 = executeWith(Configs.InterpretedAndSlottedAndPipelined, query1, params = params).toList
    val result2 = executeWith(Configs.InterpretedAndSlottedAndPipelined, query2, params = params).toList

    result1.size should equal(result2.size)
  }

  test("distinct aggregation on single node") {
    val node1 = createNode()
    val node2 = createNode()
    relate(node1, node2)
    relate(node2, node1)
    val result = executeWith(Configs.All, "MATCH (a)--() RETURN DISTINCT a")

    result.toSet should equal(Set(Map("a" -> node1), Map("a" -> node2)))
  }

  test("distinct aggregation on array property") {
    createNode("prop"-> Array(42))
    createNode("prop"-> Array(42))
    createNode("prop"-> Array(1337))
    val result = executeWith(Configs.All, "MATCH (a) RETURN DISTINCT a.prop")

    result.toComparableResult.toSet should equal(Set(Map("a.prop" -> List(1337)), Map("a.prop" -> List(42))))
  }

  test("Node count from count store plan should work with labeled nodes") {
    createLabeledNode("Person")
    createLabeledNode("Person")
    createNode()
    // CountStore not supported by sloted
    val result = executeWith(Configs.FromCountStore, "MATCH (a:Person) WITH count(a) as c RETURN c")
    result.toList should equal(List(Map("c" -> 2L)))
  }

  test("Count should work with projected node variable") {
    createLabeledNode("Person")
    createLabeledNode("Person")
    createNode()
    // This does not use countstore
    val result = executeWith(Configs.All, "MATCH (a:Person) WITH a as b WITH count(b) as c RETURN c")
    result.toList should equal(List(Map("c" -> 2L)))
  }

  test("Count should work with projected relationship variable") {
    val node1 = createLabeledNode("Person")
    val node2 = createNode()
    val node3 = createNode()
    relate(node1, node2)
    relate(node1, node3)

    val result = executeWith(Configs.All, "MATCH (a:Person)-[r]->() WITH r as s WITH count(s) as c RETURN c")
    result.toList should equal(List(Map("c" -> 2L)))
  }

  test("combine grouping and aggregation with sorting") {
    val node1 = createNode(Map("prop" -> 1))
    val node2 = createNode(Map("prop" -> 2))
    relate(node1, node2)

    val result = executeWith(Configs.All, "MATCH (a)--(b) RETURN a.prop, count(a) ORDER BY a.prop")
    result.toList should equal(List(Map("a.prop" -> 1, "count(a)" -> 1), Map("a.prop" -> 2, "count(a)" -> 1)))
  }

  test("combine simple aggregation on projection with sorting") {
    createNode()
    createNode()
    val result = executeWith(Configs.All, "MATCH (a) WITH a as b RETURN count(b) ORDER BY count(b)")
    result.toList should equal(List(Map("count(b)" -> 2)))
  }

  test("combine simple aggregation with sorting (cannot use count store)") {
    createNode(Map("prop" -> 1))
    createNode(Map("prop" -> 2))
    val result = executeWith(Configs.All, "MATCH (a) RETURN count(a.prop) ORDER BY count(a.prop)")
    result.toList should equal(List(Map("count(a.prop)" -> 2)))
  }

  test("combine simple aggregation with sorting (can use node count store)") {
    createNode()
    createNode()
    val result = executeWith(Configs.FromCountStore, "MATCH (a) RETURN count(a) ORDER BY count(a)")
    result.toList should equal(List(Map("count(a)" -> 2)))
  }

  test("combine simple aggregation with sorting (can use relationship count store)") {
    val node1 = createNode()
    val node2 = createNode()
    relate(node1, node2)
    val result = executeWith(Configs.All, "MATCH (a)-[r]-(b) RETURN count(r) ORDER BY count(r)")
    result.toList should equal(List(Map("count(r)" -> 2)))
  }

  test("should support DISTINCT followed by LIMIT and SKIP") {
    createNode(Map("prop" -> 1))
    val node2 = createNode(Map("prop" -> 2))
    val query = "MATCH (a) RETURN DISTINCT a ORDER BY a.prop SKIP 1 LIMIT 1"

    val result = executeWith(Configs.All, query)

    result.toList should equal(List(Map("a" -> node2)))
  }

  test("should support DISTINCT projection followed by LIMIT and SKIP") {
    createNode(Map("prop" -> 1))
    createNode(Map("prop" -> 2))
    val query = "MATCH (a) RETURN DISTINCT a.prop ORDER BY a.prop SKIP 1 LIMIT 1"

    val result = executeWith(Configs.All, query)

    result.toList should equal(List(Map("a.prop" -> 2)))
  }

  test("should support DISTINCT projection followed by SKIP") {
    createNode(Map("prop" -> 1))
    createNode(Map("prop" -> 2))
    val query = "MATCH (a) RETURN DISTINCT a.prop ORDER BY a.prop SKIP 1"

    val result = executeWith(Configs.All, query)

    result.toList should equal(List(Map("a.prop" -> 2)))
  }

  test("should support DISTINCT projection followed by LIMIT") {
    createNode(Map("prop" -> 1))
    createNode(Map("prop" -> 2))
    val query = "MATCH (a) RETURN DISTINCT a.prop ORDER BY a.prop LIMIT 1"

    val result = executeWith(Configs.All, query)

    result.toList should equal(List(Map("a.prop" -> 1)))
  }

  test("should support DISTINCT followed by LIMIT and SKIP with no ORDER BY") {
    createNode(Map("prop" -> 1))
    createNode(Map("prop" -> 2))
    val query = "MATCH (a) WITH DISTINCT a SKIP 1 LIMIT 1 RETURN count(a)"

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList should equal(List(Map("count(a)" -> 1)))
  }

  test("grouping and ordering with multiple different types that can all be represented by primitives") {
    val node1 = createNode(Map("prop" -> 1))
    val node2 = createNode(Map("prop" -> 2))
    val r1 = relate(node1, node2)

    val query = "MATCH (a)-[r]-(b) RETURN a, r, b, count(a) ORDER BY a, r, b"
    val result = executeWith(Configs.All, query) // Neo4j version <= 3.1 cannot order by nodes
    result.toList should equal(List(
      Map("a" -> node1, "r" -> r1, "b" -> node2, "count(a)" -> 1),
      Map("a" -> node2, "r" -> r1, "b" -> node1, "count(a)" -> 1)
    ))
  }

  test("grouping and ordering with multiple different types with mixed representations") {
    val node1 = createNode(Map("prop" -> "alice"))
    val node2 = createNode(Map("prop" -> "bob"))
    val r1 = relate(node1, node2)

    val query = "MATCH (a)-[r]-(b) RETURN a, r, b, a.prop as s, count(a) ORDER BY a, r, b, s"
    val result = executeWith(Configs.All, query) // Neo4j version <= 3.1 cannot order by nodes
    result.toList should equal(List(
      Map("a" -> node1, "r" -> r1, "b" -> node2, "s" -> "alice", "count(a)" -> 1),
      Map("a" -> node2, "r" -> r1, "b" -> node1, "s" -> "bob", "count(a)" -> 1)
    ))
  }

  test("grouping and ordering with multiple different Value types") {
    val node1 = createNode(Map("prop" -> "alice"))
    val node2 = createNode(Map("prop" -> "bob"))
    relate(node1, node2)

    val query = "MATCH (a)-[r]-(b) RETURN a.prop, b.prop, count(a) ORDER BY a.prop, b.prop"
    val result = executeWith(Configs.All, query) // Neo4j version <= 3.1 cannot order by nodes
    result.toList should equal(List(
      Map("a.prop" -> "alice", "b.prop" -> "bob", "count(a)" -> 1),
      Map("a.prop" -> "bob", "b.prop" -> "alice", "count(a)" -> 1)
    ))
  }

  test("Should sum durations") {
    val query = "UNWIND [duration('PT10S'), duration('P1D'), duration('PT30.5S')] as x RETURN sum(x) AS length"
    executeWith(Configs.UDF, query).toList should equal(List(Map("length" -> DurationValue.duration(0,1,40,500000000))))
  }

  test("Should sum durations from stored nodes") {
    createNode(Map("d" -> DurationValue.duration(0,0,10,0)))
    createNode(Map("d" -> DurationValue.duration(0,1,0,0)))
    createNode(Map("d" -> DurationValue.duration(0,0,30,500000000)))

    val query = "MATCH (n) RETURN sum(n.d) AS length"
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query).toList should equal(List(Map("length" -> DurationValue.duration(0, 1, 40, 500000000))))
  }

  test("Should not sum durations and numbers together") {
    val query = "UNWIND [duration('PT10S'), duration('P1D'), duration('PT30.5S'), 90] as x RETURN sum(x) AS length"
    failWithError(Configs.UDF, query, Seq("cannot mix number and duration"))
  }

  test("Should avg durations") {
    val query = "UNWIND [duration('PT10S'), duration('P3D'), duration('PT20.6S')] as x RETURN avg(x) AS length"
    executeWith(Configs.UDF, query).toList should equal(List(Map("length" -> DurationValue.duration(0,1,10,200000000))))
  }

  test("Should avg durations from stored nodes") {
    createNode(Map("d" -> DurationValue.duration(0,0,10,0)))
    createNode(Map("d" -> DurationValue.duration(0,3,0,0)))
    createNode(Map("d" -> DurationValue.duration(0,0,20,600000000)))

    val query = "MATCH (n) RETURN avg(n.d) AS length"
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query).toList should equal(List(Map("length" -> DurationValue.duration(0, 1, 10, 200000000))))
  }

  test("Should not avg durations and numbers together") {
    val query = "UNWIND [duration('PT10S'), duration('P1D'), duration('PT30.5S'), 90] as x RETURN avg(x) AS length"
    failWithError(Configs.UDF, query, Seq("cannot mix number and duration"))
  }

  test("should give correct scope for ORDER BY following aggregation with shadowing of variable") {
    val query = """WITH 1 AS foo, 2 AS bar
                  |WITH foo AS foo,
                  |     head(collect(bar)) AS agg
                  |  ORDER BY foo ASC
                  |RETURN *""".stripMargin
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query).toList should equal(List(Map("agg" -> 2, "foo" -> 1)))
  }

  test("should give correct scope for ORDER BY following aggregation with shadowing of variable II") {
    val query = """WITH 1 AS foo, 2 AS bar
                  |WITH foo AS foo,
                  |    collect(bar) AS agg
                  |  ORDER BY foo ASC
                  |RETURN *""".stripMargin
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query).toList should equal(List(Map("agg" -> List(2), "foo" -> 1)))
  }

  test("should give correct scope for WHERE following aggregation with shadowing of variable") {
    val query = """WITH 1 AS foo, 2 AS bar
                  |WITH foo AS foo,
                  |     head(collect(bar)) AS agg
                  |  WHERE foo = 1
                  |RETURN *""".stripMargin
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query).toList should equal(List(Map("agg" -> 2, "foo" -> 1)))
  }

  test("should give correct scope for ORDER BY following distinct with shadowing of variable") {
    val query = """WITH 1 AS foo, 2 AS bar
                  |WITH DISTINCT foo AS foo,
                  |    bar AS bar
                  |  ORDER BY foo ASC
                  |RETURN *""".stripMargin
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query).toList should equal(List(Map("bar" -> 2, "foo" -> 1)))
  }

  test("should give correct scope for ORDER BY following distinct with shadowing of variable II") {
    val query = """WITH 1 AS foo, 2 AS bar
                  |WITH DISTINCT foo AS foo,
                  |     head(collect(bar)) AS agg
                  |  ORDER BY foo ASC
                  |RETURN *""".stripMargin
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query).toList should equal(List(Map("agg" -> 2, "foo" -> 1)))
  }

  test("should give correct scope for ORDER BY following distinct with shadowing of variable III") {
    val query = """WITH 1 AS foo, 2 AS bar
                  |WITH DISTINCT foo + foo AS foo
                  |  ORDER BY foo ASC
                  |RETURN *""".stripMargin
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query).toList should equal(List(Map("foo" -> 2)))
  }

  test("should not be able to see variable after distinct") {
    val query = """WITH 1 AS foo, 2 AS bar
                  |WITH DISTINCT foo + bar AS foobar
                  |  ORDER BY foo ASC
                  |RETURN *""".stripMargin
    val error = the[SyntaxException] thrownBy {
      executeSingle(query)
    }

    error.getMessage should startWith("In a WITH/RETURN with DISTINCT or an aggregation, " +
                                      "it is not possible to access variables declared before the WITH/RETURN:")
  }

  test("should give correct scope for ORDER BY following aggregation") {
    val query = """WITH 1 AS foo, 2 AS bar
                  |WITH foo + bar AS foobar,
                  |     head(collect(bar)) AS agg
                  |  ORDER BY foobar ASC
                  |RETURN *""".stripMargin
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query).toList should equal(List(Map("agg" -> 2, "foobar" -> 3)))
  }

  test("should give correct scope for ORDER BY following aggregation and distinct") {
    val query = """WITH 1 AS foo, 2 AS bar
                  |WITH DISTINCT foo + bar AS foobar,
                  |     head(collect(bar)) AS agg
                  |  ORDER BY foobar ASC
                  |RETURN *""".stripMargin
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query).toList should equal(List(Map("agg" -> 2, "foobar" -> 3)))
  }

  test("should give correct scope for WHERE following aggregation") {
    val query = """WITH 1 AS foo, 2 AS bar
                  |WITH foo + bar AS foobar,
                  |     head(collect(bar)) AS agg
                  |  WHERE foobar = 3
                  |RETURN *""".stripMargin
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query).toList should equal(List(Map("agg" -> 2, "foobar" -> 3)))
  }

  test("collect processes rows after an ORDER BY in that order") {
    //given
    executeSingle("""CREATE (a {name: 'A', age: 34}),
                    |       (b {name: 'B', age: 36}),
                    |       (c {name: 'C', age: 32})""".stripMargin)

    // when
    val query =
      """MATCH (n)
        |WITH n ORDER BY n.age
        |RETURN collect(n.name) AS names""".stripMargin

    // then
   executeWith(Configs.InterpretedAndSlottedAndPipelined, query).toList should equal(List(Map("names" -> List("C", "A", "B"))))
  }

  test("should not inject shadowed variables into for-comprehension") {
    val query =
      """
        |WITH {name: 'a_name', prop: 'a_prop'} AS a,
        |     {name: 'b_name', prop: 'b_prop'} AS b
        |RETURN a.name, [a IN collect(b) | [a.name, a.prop]] AS r
        |""".stripMargin

    executeWith(Configs.InterpretedAndSlottedAndPipelined, query).toSet should equal(Set(
      Map("a.name" -> "a_name", "r" -> Seq(Seq("b_name", "b_prop")))
    ))
  }

  test("should not inject shadowed variables into reduce") {
    val query =
      """
        |WITH {name: 'a_name', prop: 'a_prop'} AS a,
        |     {name: 'b_name', prop: 'b_prop'} AS b
        |RETURN a.name, reduce(acc = '', a IN collect(b) | acc + a.name  + ':' + a.prop) AS r
        |""".stripMargin

    executeWith(Configs.InterpretedAndSlottedAndPipelined, query).toSet should equal(Set(
      Map("a.name" -> "a_name", "r" -> "b_name:b_prop")
    ))
  }

  test("should handle WITH * followed by aggregation and order by") {
    val query = "UNWIND [1, 2, 2, 3, 3, 3, 4, 4, 4, 4] AS n WITH *, count(n) AS c ORDER BY c RETURN n, c"
    val result = executeWith(Configs.All, query)
    result.toList should be(List(
      Map("n" -> 1, "c" -> 1),
      Map("n" -> 2, "c" -> 2),
      Map("n" -> 3, "c" -> 3),
      Map("n" -> 4, "c" -> 4)
    ))
  }
}
