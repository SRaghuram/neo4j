/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.queryReduction

import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.ArithmeticException
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

import scala.util.{Failure, Success, Try}

class CypherReductionSupportTest extends CypherFunSuite with CypherReductionSupport {

  private val NL = System.lineSeparator()

  test("a simply query that cannot be reduced") {
    val query = "MATCH (n) RETURN n"
    reduceQuery(query)(_ => NotReproduced) should equal(s"MATCH (n)${NL}RETURN n AS n")
  }

  test("removes unnecessary where") {
    val setup = "CREATE (n {name: \"x\"}) RETURN n"
    val query = "MATCH (n) WHERE true RETURN n.name"
    val reduced = reduceQuery(query, Some(setup)) { tryResults: Try[RewindableExecutionResult] =>
      tryResults match {
        case Success(result) =>
          val list = result.toList
          if(list.nonEmpty && list.head == Map("n.name" -> "x"))
            Reproduced
          else
            NotReproduced
        case Failure(_) => NotReproduced
      }
    }
    reduced should equal(s"MATCH (n)${NL}RETURN n.name AS `n.name`")
  }

  test("rolls back after each oracle invocation") {
    val query = "CREATE (n) RETURN n"
    reduceQuery(query)(_ => NotReproduced)
    evaluate("MATCH (n) RETURN count(n)").toList should be(List(Map("count(n)" -> 0)))
  }

  test("evaluate rolls back") {
    val query = "CREATE (n) RETURN n"
    evaluate(query)
    evaluate("MATCH (n) RETURN count(n)").toList should be(List(Map("count(n)" -> 0)))
  }

  test("removes unnecessary stuff from faulty query") {
    val setup = "CREATE (n:Label {name: 0}) RETURN n"
    val query = s"MATCH (n:Label)-[:X]->(m:Label),(p) WHERE 100/n.name > 34 AND m.name = n.name WITH n.name AS name RETURN name, $$a ORDER BY name SKIP 1 LIMIT 5"
    val reduced = reduceQuery(query, Some(setup)) { tryResults: Try[RewindableExecutionResult] =>
      tryResults match {
        case Failure(e:ArithmeticException) =>
          if(e.getMessage == "/ by zero" || e.getMessage == "divide by zero")
            Reproduced
          else
            NotReproduced
        case _ => NotReproduced
      }
    }
    reduced should equal(s"MATCH (n)$NL  WHERE 100 / n.name > 34${NL}RETURN ")
  }

  test("removes unnecessary stuff from faulty query with enterprise") {
    val setup = "CREATE (n:Label {name: 0}) RETURN n"
    val query = s"MATCH (n:Label)-[:X]->(m:Label),(p) WHERE 100/n.name > 34 AND m.name = n.name WITH n.name AS name RETURN name, $$a ORDER BY name SKIP 1 LIMIT 5"
    val reduced = reduceQuery(query, Some(setup), enterprise = true) { tryResults: Try[RewindableExecutionResult] =>
      tryResults match {
        case Failure(e:ArithmeticException) =>
          if(e.getMessage == "/ by zero" || e.getMessage == "divide by zero")
            Reproduced
          else
            NotReproduced
        case _ => NotReproduced
      }
    }
    reduced should equal(s"MATCH (n)$NL  WHERE 100 / n.name > 34${NL}RETURN ")
  }
}
