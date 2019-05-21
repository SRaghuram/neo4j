/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.utils

import com.neo4j.fabric.pipeline.Pipeline
import com.neo4j.fabric.utils.Rewritten._
import com.neo4j.fabric.{AstHelp, Test}
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.ast.prettifier.{ExpressionStringifier, Prettifier}
import org.neo4j.cypher.internal.v4_0.util.ASTNode
import org.scalacheck.Shrink
import org.scalatest.Assertion
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class AstGeneratorTest extends Test with AstHelp with GeneratorDrivenPropertyChecks {

  val gen = AstGenerator()

  val pr = Prettifier(ExpressionStringifier(alwaysParens = true))

  implicit val shrink = Shrink[Query] { q =>
    q.part match {
      case sq: SingleQuery if sq.clauses.size > 1 =>
        sq.clauses.indices.toStream.map(i =>
          q.copy(
            part = sq.copy(
              clauses = Seq(sq.clauses(i))
            )(sq.position)
          )(q.position)
        )

      case _ => Stream.empty
    }
  }

  def pretty(c: Clause): String =
    pr.asString(Query(None, SingleQuery(Seq(c))(?))(?))

  def sidebyside(a: Any, b: Option[Any]): Unit = {
    val width = 60
    val height = 1000
    b match {
      case Some(bb) => sidebyside(a, bb, width, height)
      case None     => pprint.pprintln(a, width = width, height = height)
    }
  }

  def sidebyside(a: Any, b: Any, width: Int, height: Int): Unit = {
    val as = pprint.apply(a, width = width, height = height).render.linesIterator
    val bs = pprint.apply(b, width = width, height = height).render.linesIterator

    fansi.Str.apply("dada").length
    for {
      (l, r) <- as.zipAll(bs, "", "")
      printedWidth = fansi.Str.ansiRegex.matcher(l).replaceAll("").length
      lp = l + " " * (width - printedWidth)
      sep = if (l == r) "|" else "X"
      line = lp + sep + r
    } println(line)
  }


  def roundTripCheck(query: String): Assertion = {
    val q = Pipeline.parseOnly.process(query).statement()
    val qs = pr.asString(q)
    val qp = Pipeline.parseOnly.process(qs).statement()
    try {
      q shouldEqual qp
    } catch {
      case e: Exception =>
        println(query)
        println(qs)
        sidebyside(q, Some(qp))
        throw e
    }
  }

  def roundTripCheck(q: Statement): Assertion = {
    val qs = pr.asString(q)
    val qp = try {
      Pipeline.parseOnly.process(qs).statement()
    } catch {
      case e: Exception =>
        println(qs)
        sidebyside(q, None)
        throw e
    }
    val qf = dropQuotedSyntax(qp)
    try {
      q shouldEqual qf
    } catch {
      case e: Exception =>
        println(qs)
        sidebyside(q, Some(qf))
        throw e
    }
  }

  def dropQuotedSyntax[T <: ASTNode](n: T): T = n.rewritten.bottomUp {
    case i @ UnaliasedReturnItem(e, _) => UnaliasedReturnItem(e, "")(i.position)
  }

  "ORDER BY SKIP" in {
    Pipeline.parseOnly.process("ORDER BY SKIP 1")
  }

  "hints" in {
    roundTripCheck("MATCH (a:X) USING INDEX b:Y(v, u) USING INDEX SEEK c:Z(x, y)")
    roundTripCheck("MATCH (a:X) USING SCAN b:Y USING SCAN c:Z")
    roundTripCheck("MATCH (a:X) USING JOIN ON a, b USING JOIN ON c")
  }

  "COPY OF" in {
    roundTripCheck("MATCH (a COPY OF b)")
    roundTripCheck("MATCH ()-[a COPY OF b]-()")
  }

  "RETURN *" in {
    roundTripCheck("RETURN *")
    roundTripCheck("RETURN *, a")
    roundTripCheck("WITH *")
    roundTripCheck("WITH *, a")
  }

  def show(q: String) = {
    println("orig: " + q)
    val statement = Pipeline.parseOnly.process(q).statement()
    println("stmt: " + statement)
    val pretty = pr.asString(statement)
    println("pret: " + pretty)
    val statement2 = Pipeline.parseOnly.process(pretty).statement()
    println("stmt: " + statement2)
  }

  "ands" in {
    show("RETURN x > y = z")
    show("RETURN x > y AND y = z")
    show("RETURN x > (y = z)")
  }


  "gen" in {
    forAll(gen._query) { q =>
      roundTripCheck(q)
    }
  }

}
