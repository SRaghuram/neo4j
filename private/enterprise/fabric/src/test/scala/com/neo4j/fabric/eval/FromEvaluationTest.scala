/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import java.net.URI
import java.time.Duration
import java.util

import com.neo4j.fabric.config.FabricConfig
import com.neo4j.fabric.config.FabricConfig.{GlobalDriverConfig, Graph}
import com.neo4j.fabric.eval.Catalog.RemoteGraph
import com.neo4j.fabric.pipeline.SignatureResolver
import com.neo4j.fabric.{FabricTest, ProcedureRegistryTestSupport}
import org.neo4j.cypher.internal.v4_0.ast.FromGraph
import org.neo4j.cypher.internal.v4_0.parser.{Clauses, Query}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.TestName
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.parboiled.scala.ReportingParseRunner

import scala.collection.mutable

class FromEvaluationTest extends FabricTest with ProcedureRegistryTestSupport with TestName {

  private val mega0 = new Graph(0L, URI.create("bolt://mega:1111"), "neo4j", "source_of_all_truth", null)
  private val mega1 = new Graph(1L, URI.create("bolt://mega:2222"), "neo4j", null, null)
  private val mega2 = new Graph(2L, URI.create("bolt://mega:3333"), "neo4j", "mega", null)

  private val config = new FabricConfig(
    true,
    new FabricConfig.Database("mega", util.Set.of(mega0, mega1, mega2)),
    util.List.of(), 0L, Duration.ZERO,
    new GlobalDriverConfig(Duration.ZERO, Duration.ZERO, 0, null),
    new FabricConfig.DataStream(300, 1000, 50)
  )

  "Correctly evaluates:" - {
    "FROM mega.graph0" in { eval() shouldEqual RemoteGraph(mega0) }
    "FROM mega.graph1" in { eval() shouldEqual RemoteGraph(mega1) }
    "FROM mega.graph(0)" in { eval() shouldEqual RemoteGraph(mega0) }
    "FROM mega.graph(1)" in { eval() shouldEqual RemoteGraph(mega1) }
    "FROM mega.graph(const0())" in { eval() shouldEqual RemoteGraph(mega0) }
    "FROM mega.graph(const1())" in { eval() shouldEqual RemoteGraph(mega1) }
    "FROM mega.graph(x)" in { eval("x" -> Values.intValue(0)) shouldEqual RemoteGraph(mega0) }
    "FROM mega.graph(y)" in { eval("y" -> Values.intValue(1)) shouldEqual RemoteGraph(mega1) }
    "FROM mega.source_of_all_truth" in { eval() shouldEqual RemoteGraph(mega0) }
    "FROM mega.mega" in { eval() shouldEqual RemoteGraph(mega2) }
  }

  object eval {

    private object parse extends Query with Clauses {
      def apply(from: String): FromGraph =
        ReportingParseRunner(this.FromGraph).run(from).result.get
    }

    private val catalog = Catalog.fromConfig(config)
    private val signatures = new SignatureResolver(() => procedures)
    private val evaluation = FromEvaluation(catalog, () => procedures, signatures)

    def queryFromTestName: String =
      testName.split(":", 2).last.trim

    def apply(
      vars: (String, AnyValue)*
    ): Catalog.Graph = eval(queryFromTestName, vars: _*)

    def apply(
      params: MapValue,
      context: mutable.Map[String, AnyValue]
    ): Catalog.Graph = eval(queryFromTestName, params, context)

    def eval(
      from: String,
      vars: (String, AnyValue)*
    ): Catalog.Graph = eval(from, MapValue.EMPTY, mutable.Map(vars: _*))

    def eval(
      from: String,
      params: MapValue,
      context: mutable.Map[String, AnyValue]
    ): Catalog.Graph = evaluation.evaluate(parse(from), params, context)
  }



}
