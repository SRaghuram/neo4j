/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import java.time.Duration
import java.util

import com.neo4j.fabric.config.FabricConfig
import com.neo4j.fabric.config.FabricConfig.{GlobalDriverConfig, Graph}
import com.neo4j.fabric.eval.Catalog.RemoteGraph
import com.neo4j.fabric.pipeline.SignatureResolver
import com.neo4j.fabric.{FabricTest, ProcedureRegistryTestSupport}
import org.neo4j.configuration.helpers.NormalizedDatabaseName
import org.neo4j.cypher.internal.v4_0.ast.UseGraph
import org.neo4j.cypher.internal.v4_0.parser.{Clauses, Query}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.TestName
import org.neo4j.exceptions.EntityNotFoundException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.parboiled.scala.ReportingParseRunner

import scala.collection.mutable

class UseEvaluationTest extends FabricTest with ProcedureRegistryTestSupport with TestName {

  private val mega0 = new Graph(0L, FabricConfig.RemoteUri.create("bolt://mega:1111"), "neo4j", "source_of_all_truth", null)
  private val mega1 = new Graph(1L, FabricConfig.RemoteUri.create("bolt://mega:2222"), "neo4j", null, null)
  private val mega2 = new Graph(2L, FabricConfig.RemoteUri.create("bolt://mega:3333"), "neo4j", "mega", null)

  private val config = new FabricConfig(
    true,
    new FabricConfig.Database(new NormalizedDatabaseName("mega"), util.Set.of(mega0, mega1, mega2)),
    util.List.of(), Duration.ZERO, Duration.ZERO,
    new GlobalDriverConfig(Duration.ZERO, Duration.ZERO, 0, null),
    new FabricConfig.DataStream(300, 1000, 50)
  )

  "Correctly evaluates:" - {
    "USE mega.graph(0)" in { eval() shouldEqual RemoteGraph(mega0) }
    "USE mega.graph(1)" in { eval() shouldEqual RemoteGraph(mega1) }
    "USE mega.graph(const0())" in { eval() shouldEqual RemoteGraph(mega0) }
    "USE mega.graph(const1())" in { eval() shouldEqual RemoteGraph(mega1) }
    "USE mega.graph(x)" in { eval("x" -> Values.intValue(0)) shouldEqual RemoteGraph(mega0) }
    "USE mega.graph(y)" in { eval("y" -> Values.intValue(1)) shouldEqual RemoteGraph(mega1) }
    "USE mega.source_of_all_truth" in { eval() shouldEqual RemoteGraph(mega0) }
    "USE mega.mega" in { eval() shouldEqual RemoteGraph(mega2) }
    "USE meGA.Graph(0)" in { eval() shouldEqual RemoteGraph(mega0) }
    "USE MEGA.GRAPH(1)" in { eval() shouldEqual RemoteGraph(mega1) }
    "USE Mega.Graph(Const0())" in { eval() shouldEqual RemoteGraph(mega0) }
    "USE MEGA.GRAPH(CONST1())" in { eval() shouldEqual RemoteGraph(mega1) }
    "USE mega.Graph(x)" in { eval("x" -> Values.intValue(0)) shouldEqual RemoteGraph(mega0) }
    "USE mega.GRAPH(y)" in { eval("y" -> Values.intValue(1)) shouldEqual RemoteGraph(mega1) }
    "USE mega.sOuRce_Of_aLL_tRuTH" in { eval() shouldEqual RemoteGraph(mega0) }
    "USE Mega.MEGA" in { eval() shouldEqual RemoteGraph(mega2) }
  }

  "Fails for:" - {
    "USE mega.graph0" in { the[EntityNotFoundException].thrownBy(eval()).getMessage.should(include("not found: mega.graph0")) }
    "USE mega.graph1" in { the[EntityNotFoundException].thrownBy(eval()).getMessage.should(include("not found: mega.graph1")) }
    "USE mega.graph(10)" in { the[EntityNotFoundException].thrownBy(eval()).getMessage.should(include("not found: 10")) }
    "USE mega.GRAph0" in { the[EntityNotFoundException].thrownBy(eval()).getMessage.should(include("not found: mega.GRAph0")) }
    "USE MEGA.graph1" in { the[EntityNotFoundException].thrownBy(eval()).getMessage.should(include("not found: MEGA.graph1")) }
  }

  object eval {

    private object parse extends Query with Clauses {
      def apply(use: String): UseGraph =
        ReportingParseRunner(this.UseGraph).run(use).result.get
    }

    private val catalog = Catalog.fromConfig(config)
    private val signatures = new SignatureResolver(() => procedures)
    private val evaluation = UseEvaluation(catalog, () => procedures, signatures)

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
      use: String,
      vars: (String, AnyValue)*
    ): Catalog.Graph = eval(use, MapValue.EMPTY, mutable.Map(vars: _*))

    def eval(
      use: String,
      params: MapValue,
      context: mutable.Map[String, AnyValue]
    ): Catalog.Graph = evaluation.evaluate(use, parse(use), params, context)
  }



}
