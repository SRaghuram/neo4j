/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher.parser

import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.factory.neo4j.Neo4jASTExceptionFactory
import org.neo4j.cypher.internal.ast.factory.neo4j.Neo4jASTFactory
import org.neo4j.cypher.internal.parser.javacc.CypherCharStream
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory

abstract class BaseParserBenchmark extends BaseDatabaseBenchmark {

  override def benchmarkGroup(): String = "Cypher"

  override def isThreadSafe: Boolean = false
}

abstract class BaseParserState {
  var parser: String => Statement = _

  protected def setUpParser(parserImpl: String): Unit = {
    val exceptionFactory = new OpenCypherExceptionFactory(None)
    parser =
      parserImpl match {
        case "parboiled" =>
          val x = new org.neo4j.cypher.internal.parser.CypherParser()
          query => x.parse(query, exceptionFactory)
        case "javacc" =>
          query => {
            val x = new org.neo4j.cypher.internal.parser.javacc.Cypher(
              new Neo4jASTFactory(query),
              new Neo4jASTExceptionFactory(exceptionFactory),
              new CypherCharStream(query))
            x.Statements().get(0)
          }
      }
  }

  def parseQuery(query: String): Statement = {
    try {
      parser(query)
    } catch {
      case e: NullPointerException => throw e
      case _: Throwable => null // ignore, some queries are meant to fail
    }
  }

  protected def tearDownParser(): Unit = {
    parser = null
  }
}
