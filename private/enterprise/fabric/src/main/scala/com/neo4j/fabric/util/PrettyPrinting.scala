/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.util

import org.neo4j.cypher.internal.ast.prettifier.{ExpressionStringifier, Prettifier}
import org.neo4j.cypher.internal.ast.{Clause, Query, SingleQuery, Statement}
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.util.InputPosition

object PrettyPrinting extends PrettyPrintingUtils

trait PrettyPrinting[T] extends PrettyPrintingUtils {
  def pretty: T => Stream[String]

  def node(name: String, fields: Seq[(String, Any)], children: Seq[T] = Seq()): Stream[String] = {
    def head(name: String) = Stream(s"[ $name ]")

    def middle(fields: Seq[(String, Any)]): Stream[String] = {
      val max = if (fields.nonEmpty) fields.map(_._1.length).max else 0

      fields.toStream.flatMap {
        case (name, vs: Stream[_]) =>
          val text = vs.map(e => "  ┊ " + e.toString)
          Stream(s"╞ $name:") ++ text
        case (name, value)         =>
          val space = " " * (max - name.length)
          Stream(s"╞ $name$space: $value")
      }
    }

    def rest(cs: Seq[T]) = cs match {
      case Seq() => Stream()
      case es    =>
        es.init.toStream.flatMap(c => framing(pretty(c), "├─ ", "│    ")) ++
          framing(pretty(es.last), "└─ ", "     ")
    }

    def framing(in: Stream[String], first: String, rest: String): Stream[String] = {
      val head = first + in.head
      val tail = in.tail.map(rest + _)
      head #:: tail
    }

    head(name) ++ middle(fields) ++ rest(children)
  }

  def pprint(t: T): Unit =
    pretty(t).foreach(println)

  def asString(t: T): String =
    pretty(t).mkString(System.lineSeparator())
}
trait PrettyPrintingUtils {

  private val printer = Prettifier(ExpressionStringifier())

  def expr(e: Expression): String =
    printer.expr.apply(e)

  def query(s: Statement): Stream[String] =
    printer.asString(s).linesIterator.toStream

  def clause(c: Clause): String =
    query(Seq(c)).mkString("\n")

  def query(cs: Seq[Clause]): Stream[String] = {
    val pos = InputPosition.NONE
    query(Query(None, SingleQuery(cs)(pos))(pos))
  }

  def list(ss: Seq[Any]): String =
    ss.mkString(",")
}
