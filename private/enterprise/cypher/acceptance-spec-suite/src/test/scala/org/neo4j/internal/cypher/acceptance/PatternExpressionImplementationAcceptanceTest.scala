/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.ExpandExpression
import org.neo4j.cypher.internal.runtime.PathImpl
import org.neo4j.graphdb.Node
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.scalatest.Matchers

class PatternExpressionImplementationAcceptanceTest extends ExecutionEngineFunSuite with Matchers with CypherComparisonSupport {

  // TESTS WITH CASE EXPRESSION

  test("match (n) return case when id(n) >= 0 then (n)-->() otherwise 42 as p") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.NestedPlan, "match (n) return case when id(n) >= 0 then (n)-->() else 42 end as p",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("Expand(All)")))

    result.toList.head("p").asInstanceOf[Seq[_]] should have size 2
  }

  test("pattern expression without any bound nodes") {

    val start = createLabeledNode("A")
    relate(start, createNode())
    relate(start, createNode())

    val query = "return case when true then (:A)-->() else 42 end as p"
    val result = executeWith(Configs.NestedPlan, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("Expand(All)")))

    result.toList.head("p").asInstanceOf[Seq[_]] should have size 2
  }

  test("match (n) return case when id(n) < 0 then (n)-->() otherwise 42 as p") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.NestedPlan, "match (n) return case when id(n) < 0 then (n)-->() else 42 end as p",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("Expand(All)")))

    result.toList.head("p").asInstanceOf[Long] should equal(42)
  }

  test("match (n) return case when n:A then (n)-->(:C) when n:B then (n)-->(:D) else 42 end as p") {
    val start = createLabeledNode("A")
    val c = createLabeledNode("C")
    val rel1 = relate(start, c)
    val rel2 = relate(start, c)
    val start2 = createLabeledNode("B")
    val d = createLabeledNode("D")
    val rel3 = relate(start2, d)
    val rel4 = relate(start2, d)

    val result = executeWith(Configs.NestedPlan, "match (n) return case when n:A then (n)-->(:C) when n:B then (n)-->(:D) else 42 end as p")
      .toList.map(_.mapValues {
      case l: Seq[Any] => l.toSet
      case x => x
    }).toSet

    result should equal(Set(
      Map("p" -> Set(PathImpl(start, rel2, c), PathImpl(start, rel1, c))),
      Map("p" -> 42),
      Map("p" -> Set(PathImpl(start2, rel4, d), PathImpl(start2, rel3, d))),
      Map("p" -> 42)
    ))
  }

  test("match (n) with case when id(n) >= 0 then (n)-->() else 42 end as p, count(n) as c return p, c") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.NestedPlan,
      """match (n)
        |with case
        |       when id(n) >= 0 then (n)-->()
        |       else 42
        |     end as p, count(n) as c
        |return p, c order by c""".stripMargin)
      .toList.head("p").asInstanceOf[Seq[_]]

    result should have size 2
  }

  test("match (n) with case when id(n) < 0 then (n)-->() else 42 end as p, count(n) as c return p, c") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.NestedPlan, "match (n) with case when id(n) < 0 then (n)-->() else 42 end as p, count(n) as c return p, c")
      .toList.head("p").asInstanceOf[Long]

    result should equal(42)
  }

  test("match (n) with case when n:A then (n)-->(:C) when n:B then (n)-->(:D) else 42 end as p, count(n) as c return p, c") {
    val start = createLabeledNode("A")
    val c = createLabeledNode("C")
    val rel1 = relate(start, c)
    val rel2 = relate(start, c)
    val start2 = createLabeledNode("B")
    val d = createLabeledNode("D")
    val rel3 = relate(start2, d)
    val rel4 = relate(start2, d)

    val result = executeWith(Configs.NestedPlan, "match (n) with case when n:A then (n)-->(:C) when n:B then (n)-->(:D) else 42 end as p, count(n) as c return p, c")
      .toList.map(_.mapValues {
      case l: Seq[Any] => l.toSet
      case x => x
    }).toSet

    result should equal(Set(
      Map("c" -> 1, "p" -> Set(PathImpl(start, rel2, c), PathImpl(start, rel1, c))),
      Map("c" -> 1, "p" -> Set(PathImpl(start2, rel4, d), PathImpl(start2, rel3, d))),
      Map("c" -> 2, "p" -> 42)
    ))
  }

  test("match (n) where (case when id(n) >= 0 then size((n)-->()) else 42 end) > 0 return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "match (n) where (case when id(n) >= 0 then size((n)-->()) else 42 end) > 0 return n",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("RollUpApply")))

    result.toList should equal(List(
      Map("n" -> start)
    ))
  }

  test("match (n) where (case when id(n) < 0 then size((n)-->()) else 42 end) > 0 return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "match (n) where (case when id(n) < 0 then size((n)-->()) else 42 end) > 0 return n")

    result should have size 3
  }

  test("match (n) where (case when id(n) < 0 then size((n)-[:X]->()) else 42 end) > 0 return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "match (n) where (case when id(n) < 0 then size((n)-[:X]->()) else 42 end) > 0 return n")

    result should have size 3
  }

  test("match (n) where (case when id(n) < 0 then size((n)-->(:X)) else 42 end) > 0 return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.NestedPlan, "match (n) where (case when id(n) < 0 then size((n)-->(:X)) else 42 end) > 0 return n",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("RollUpApply")))

    result should have size 3
  }

  test("match (n) where (case when n:A then size((n)-->(:C)) when n:B then size((n)-->(:D)) else 42 end) > 1 return n") {
    val start = createLabeledNode("A")
    relate(start, createLabeledNode("C"))
    relate(start, createLabeledNode("C"))
    val start2 = createLabeledNode("B")
    relate(start2, createLabeledNode("D"))
    val start3 = createNode()
    relate(start3, createNode())

    val result = executeWith(Configs.NestedPlan, "match (n) where (n)-->() AND (case when n:A then size((n)-->(:C)) when n:B then size((n)-->(:D)) else 42 end) > 1 return n",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("RollUpApply")))

    result.toList should equal(List(
      Map("n" -> start),
      Map("n" -> start3)
    ))
  }

  test("MATCH (n:FOO) WITH n, COLLECT(DISTINCT { res:CASE WHEN EXISTS ((n)-[:BAR*]->()) THEN 42 END }) as x RETURN n, x") {
    val node1 = createLabeledNode("FOO")
    val node2 = createNode()
    relate(node1, node2, "BAR")
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |MATCH (n:FOO)
        |WITH n, COLLECT (DISTINCT{
        |res:CASE WHEN EXISTS((n)-[:BAR*]->()) THEN 42 END
        |}) as x RETURN n, x
      """.stripMargin
    )

    result.toList should equal(List(Map("n" -> node1, "x" -> List(Map("res" -> 42)))))
  }

  test("case expressions and pattern expressions") {
    val n1 = createLabeledNode(Map("prop" -> 42), "A")

    relate(n1, createNode())
    relate(n1, createNode())
    relate(n1, createNode())

    val result = executeWith(Configs.NestedPlan,
      """match (a:A)
        |return case
        |         WHEN a.prop = 42 THEN []
        |         ELSE (a)-->()
        |       END as X
        |         """.stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("RollUpApply")))

    result.toList should equal(List(Map("X" -> Seq())))
  }

  test("should not use full expand 1") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    executeWith(Configs.NestedPlan, "match (n) return case when id(n) >= 0 then (n)-->() else 42 end as p",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("Expand(All)")))
  }

  test("should not use full expand 2") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    executeWith(Configs.NestedPlan, "match (n) return case when id(n) < 0 then (n)-->() else 42 end as p",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("Expand(All)")))
  }

  // TESTS WITH EXTRACTING LIST COMPREHENSION

  test("match (n) return [x IN (n)-->() | head(nodes(x))]  as p") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "match (n) return [x IN (n)-->() | head(nodes(x))]  as p")

    result.toList.head("p").asInstanceOf[Seq[_]] should equal(List(start, start))
  }

  test("match (n:A) with [x IN (n)-->() | head(nodes(x))] as p, count(n) as c return p, c") {
    val start = createLabeledNode("A")
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "match (n:A) with [x IN (n)-->() | head(nodes(x))] as p, count(n) as c return p, c")
      .toList.head("p").asInstanceOf[Seq[_]]

    result should equal(List(start, start))
  }

  test("match (n) where n IN [x IN (n)-->() | head(nodes(x))] return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "match (n) where n IN [x IN (n)-->() | head(nodes(x))] return n")
      .toList

    result should equal(List(
      Map("n" -> start)
    ))
  }

  // TESTS WITH PLANNING ASSERTIONS

  test("should use full expand") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    executeWith(Configs.InterpretedAndSlottedAndPipelined, "match (n)-->(b) with (n)-->() as p, count(b) as c return p, c",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Expand(All)")))
  }

  test("should use varlength expandInto when variables are bound") {
    val a = createLabeledNode("Start")
    val b = createLabeledNode("End")
    relate(a, b)

    executeWith(Configs.InterpretedAndSlottedAndPipelined, "match (a:Start), (b:End) with (a)-[*]->(b) as path, count(a) as c return path, c",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("VarLengthExpand(Into)")))
  }

  // FAIL: <default version> <default planner> runtime=slotted returned different results than <default version> <default planner> runtime=interpreted List() did not contain the same elements as List(Map("r" -> (20000)-[T,0]->(20001)))
  test("should not use a label scan as starting point when statistics are bad") {
    (1 to 10000).foreach { _ =>
      createLabeledNode("A")
      createNode()
    }
    relate(createNode(), createLabeledNode("A"), "T")

    executeWith(Configs.InterpretedAndSlottedAndPipelined, "PROFILE MATCH ()-[r]->() WHERE ()-[r]-(:A) RETURN r",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("NodeByLabelScan")))
  }

  test("should consider cardinality input when planning pattern expression in where clause") {
    // given
    val node = createLabeledNode("A")
    (0 until 13).map(_ => createLabeledNode("A"))
    relate(node, createNode(), "HAS")

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:A) WHERE (n)-[:HAS]->() RETURN n")

    val argumentPLan = result.executionPlanDescription().cd("NodeByLabelScan")
    val estimatedRows = argumentPLan.arguments.collect { case n: EstimatedRows => n }.head
    estimatedRows should equal(EstimatedRows(14.0))
  }

  test("should consider cardinality input when planning in return") {
    // given
    val node = createLabeledNode("A")
    (0 until 18).map(_ => createLabeledNode("A"))
    val endNode = createNode()
    relate(node, endNode, "HAS")

    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:A) RETURN (n)-[:HAS]->() as p",
      planComparisonStrategy = ComparePlansWithAssertion( planDescription => {
        planDescription.find("Argument") shouldNot be(empty)
        planDescription.cd("Argument").arguments should contain(EstimatedRows(1))
        planDescription.find("Expand(All)") shouldNot be(empty)
        val expandArgs = planDescription.cd("Expand(All)").arguments.toSet
        expandArgs should contain(EstimatedRows(0.05))
        expandArgs collect {
          case ExpandExpression("n", _, Seq("HAS"), _, SemanticDirection.OUTGOING, 1, Some(1)) => true
        } should not be empty
      }))
  }

  test("should be able to execute aggregating-functions on pattern expressions") {
    // given
    val node = createLabeledNode("A")
    createLabeledNode("A")
    createLabeledNode("A")
    relate(node, createNode(), "HAS")

    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:A) RETURN count((n)-[:HAS]->()) as c",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Expand(All)")))
  }

  test("use getDegree for simple pattern expression with size clause, outgoing") {
    setup()

    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:X) WHERE size((n)-->()) > 2 RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("RollUpApply")))
  }

  test("use getDegree for simple pattern expression with size clause, incoming") {
    setup()

    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:X) WHERE size((n)<--()) > 2 RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("RollUpApply")))
  }

  test("use getDegree for simple pattern expression with size clause, both") {
    setup()

    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n:X) WHERE size((n)--()) > 2 RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("RollUpApply")))
  }

  test("use getDegree for simple pattern expression with rel-type ORs") {
    setup()

    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (n) WHERE size((n)-[:X|Y]->()) > 2 RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("RollUpApply")))
  }

  test("use getDegree for pattern expression predicate on var-length pattern") {
    // given
    graph.createIndex("MYNODE", "name")
    graph.withTx( tx => {
      tx.execute("CREATE (:MYNODE {name:'a'})-[:CONNECTED]->(:MYNODE {name:'b'})-[:CONNECTED]->(cut:MYNODE {name:'c'})-[:CONNECTED]->(:MYNODE {name:'d'})-[:CONNECTED]->(:MYNODE {name:'e'})-[:CONNECTED]->(:MYNODE {name:'z0'})").close()

      tx.execute("MATCH (cut:MYNODE {name:'c'}) CREATE (cut)<-[:HAS_CUT]-(:CUT)").close()

      tx.execute(
        """WITH range (1, 40) AS myrange
          |UNWIND myrange as i
          |WITH "z"+i AS name
          |CREATE (:MYNODE {name:name})""".stripMargin).close()

      tx.execute(
        """WITH range (1, 40) AS myrange
          |UNWIND myrange as i WITH "z"+(i-1) AS name1, "z"+i AS name2
          |MATCH (n1:MYNODE {name:name1})
          |MATCH (n2:MYNODE {name:name2})
          |CREATE (n1) -[:CONNECTED]-> (n2)""".stripMargin).close()
    })

    // when
    val query = """MATCH (mystart:MYNODE {name:'a'})
                  |MATCH path = ( (mystart) -[:CONNECTED*0..]- (other:MYNODE) )
                  |WHERE ALL(n in nodes(path) WHERE NOT (n)<-[:HAS_CUT]-() )
                  |RETURN other""".stripMargin

    val result = executeSingle(query)
    result.executionPlanDescription() shouldNot includeSomewhere.aPlan("RollUpApply")
  }

  test("solve pattern expressions in set node properties") {
    setup()

    executeWith(Configs.InterpretedAndSlotted,
      "MATCH (n) SET n.friends = size((n)<--())",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("RollUpApply")))
  }

  test("solve pattern expressions in set relationship properties") {
    setup()

    executeWith(Configs.InterpretedAndSlotted,
      "MATCH (n)-[r]-() SET r.friends = size((n)<--())",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("RollUpApply")))
  }

  test("solve pattern expressions in set node/relationship properties") {
    setup()

    executeWith(Configs.InterpretedAndSlotted,
      "MATCH (n)-[r]-() UNWIND [n,r] AS x SET x.friends = size((n)<--())",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("RollUpApply")))
  }

  test("solve pattern expressions in set node properties from map") {
    setup()

    executeWith(Configs.InterpretedAndSlotted,
      "MATCH (n) SET n += {friends: size((n)<--())}",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("RollUpApply")))
  }

  test("solve pattern expressions in set relationship properties from map") {
    setup()

    executeWith(Configs.InterpretedAndSlotted,
      "MATCH (n)-[r]-() SET r += {friends: size((n)<--())}",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot includeSomewhere.aPlan("RollUpApply")))
  }

  private def setup(): (Node, Node) = {
    val n1 = createLabeledNode("X")
    val n2 = createLabeledNode("X")

    relate(n1, createNode())
    relate(n1, createNode())
    relate(n1, createNode())
    relate(createNode(), n2)
    relate(createNode(), n2)
    relate(createNode(), n2)
    (n1, n2)
  }
}
