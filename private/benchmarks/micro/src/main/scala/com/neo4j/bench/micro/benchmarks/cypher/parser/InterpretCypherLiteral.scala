/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher.parser

import java.util

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark
import com.neo4j.bench.micro.benchmarks.RNGState
import org.neo4j.cypher.internal.ast.factory.ASTExceptionFactory
import org.neo4j.cypher.internal.ast.factory.LiteralInterpreter
import org.neo4j.cypher.internal.evaluator.Evaluator
import org.neo4j.cypher.internal.parser.javacc
import org.neo4j.cypher.internal.parser.javacc.CypherCharStream
import org.neo4j.exceptions.SyntaxException
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown

import scala.util.Random

@BenchmarkEnabled(true)
class InterpretCypherLiteral extends BaseDatabaseBenchmark {

@ParamValues(
  allowed = Array("parboiled", "javacc"),
  base = Array("parboiled", "javacc"))
@Param(Array[String]())
  var impl: String = _

  override def description = "Interpret Cypher literals"

  override def benchmarkGroup(): String = "Cypher"

  override def isThreadSafe: Boolean = false

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def interpretNumber(threadState: InterpretCypherLiteralState): Object = {
    threadState.parseLiteral(threadState.nextNumber())
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def interpretList(threadState: InterpretCypherLiteralState): Object = {
    threadState.parseLiteral(threadState.nextList())
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def interpretMap(threadState: InterpretCypherLiteralState): Object = {
    threadState.parseLiteral(threadState.nextMap())
  }
}

object InterpretCypherLiteral {

  val randomNumbers: Array[String] =
    (0 until 1000).map(i => {
      val random = new Random(12345)
      random.nextDouble().toString
    }).toArray

  val randomLists: Array[String] =
    (0 until 1000).map(i => {
      val random = new Random(12345)
      val n = 5 + random.nextInt(15)
      val sb = new StringBuilder
      sb ++= "["
      sb ++= random.nextDouble().toString
      var i = 1
      while (i < n) {
        sb += ','
        sb ++= random.nextDouble().toString
        i += 1
      }
      sb ++= "]"
      sb.result()
    }).toArray

  val randomMaps: Array[String] =
    (0 until 1000).map(i => {
      val random = new Random(12345)
      val n = 5 + random.nextInt(15)
      val sb = new StringBuilder
      sb ++= "{"
      sb ++= "v0: "
      sb ++= random.nextDouble().toString
      var i = 1
      while (i < n) {
        sb += ','
        sb += 'v'
        sb ++= i.toString
        sb += ':'
        sb ++= random.nextDouble().toString
        i += 1
      }
      sb ++= "}"
      sb.result()
    }).toArray

  def main(args: Array[String]): Unit = {
    Main.run(classOf[InterpretCypherLiteral], args:_*)
  }
}

@State(Scope.Thread)
class InterpretCypherLiteralState {
  var parser: String => Object = _
  var index: Int = 0

  @Setup
  def setUp(benchmarkState: InterpretCypherLiteral, rngState: RNGState): Unit = {
    parser =
      benchmarkState.impl match {
        case "parboiled" =>
          val parser = Evaluator.expressionEvaluator()
          literal => parser.evaluate(literal, classOf[Object])
        case "javacc" =>
          val exceptionFactory = new TestExceptionFactory()
          literal => {
            val x = new javacc.Cypher(new LiteralInterpreter(), exceptionFactory, new CypherCharStream(literal))
            x.Literal()
          }
        case str => throw new IllegalStateException(s"Unknown benchmark impl `$str`")
      }
  }

  def parseLiteral(query: String): Object = {
    try {
      parser(query)
    } catch {
      case _: Throwable => null // ignore, some queries are meant to fail
    }
  }

  def nextNumber(): String = {
    next(InterpretCypherLiteral.randomNumbers)
  }

  def nextList(): String = {
    next(InterpretCypherLiteral.randomLists)
  }

  def nextMap(): String = {
    next(InterpretCypherLiteral.randomMaps)
  }

  private def next(values: Array[String]): String = {
    index = index + 1
    if (index >= values.length) {
      index = 0
    }
    values(index)
  }

  @TearDown
  def tearDown(): Unit = {
    parser = null
  }
}

class TestExceptionFactory extends ASTExceptionFactory {

  override def syntaxException(got: String,
                               expected: util.List[String],
                               source: Exception,
                               offset: Int,
                               line: Int,
                               column: Int): Exception = new SyntaxException("", source)

  override def syntaxException(source: Exception, offset: Int, line: Int, column: Int): Exception = new SyntaxException("", source)
}
