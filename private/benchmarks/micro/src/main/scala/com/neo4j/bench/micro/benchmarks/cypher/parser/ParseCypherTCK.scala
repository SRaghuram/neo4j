/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher.parser

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.RNGState
import org.neo4j.cypher.internal.ast.Statement
import org.opencypher.tools.tck.api.CypherTCK
import org.opencypher.tools.tck.api.Execute
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
class ParseCypherTCK extends BaseParserBenchmark {

  @ParamValues(
    allowed = Array("parboiled", "javacc"),
    base = Array("parboiled", "javacc"))
  @Param(Array[String]())
  var impl: String = _

  override def description = "Parse TCK queries in random order"

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def parseNextQuery(threadState: ParseCypherTCKState): Statement = {
    threadState.parseNextQuery()
  }
}

object ParseCypherTCK {

  val shuffledTckQueries: Array[String] = {
    val tckQueries =
      for {
        scenario <- CypherTCK.allTckScenarios
        query <- scenario.steps collect { case Execute(query, _, _) => query }
      } yield query
    new Random(3).shuffle(tckQueries).toArray
  }

  def main(args: Array[String]): Unit = {
    Main.run(classOf[ParseCypherTCK], args:_*)
  }
}

@State(Scope.Thread)
class ParseCypherTCKState extends BaseParserState {
  var queryIndex: Int = 0

  @Setup
  def setUp(benchmarkState: ParseCypherTCK, rngState: RNGState): Unit = {
    setUpParser(benchmarkState.impl)
    queryIndex = 0
  }

  def parseNextQuery(): Statement = {
    queryIndex = (queryIndex + 1) % ParseCypherTCK.shuffledTckQueries.length
    parseQuery(ParseCypherTCK.shuffledTckQueries(queryIndex))
  }

  @TearDown
  def tearDown(): Unit = {
    tearDownParser()
  }
}

