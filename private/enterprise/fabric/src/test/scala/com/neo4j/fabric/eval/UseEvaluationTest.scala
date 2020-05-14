/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import java.time.Duration
import java.util
import java.util.UUID

import com.neo4j.fabric.config.FabricEnterpriseConfig
import com.neo4j.fabric.config.FabricEnterpriseConfig.GlobalDriverConfig
import com.neo4j.fabric.config.FabricEnterpriseConfig.Graph
import org.neo4j.configuration.helpers.NormalizedDatabaseName
import org.neo4j.configuration.helpers.NormalizedGraphName
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.parser.Clauses
import org.neo4j.cypher.internal.parser.Query
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.EntityNotFoundException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.fabric.FabricTest
import org.neo4j.fabric.ProcedureSignatureResolverTestSupport
import org.neo4j.fabric.config.FabricConfig
import org.neo4j.fabric.eval.Catalog
import org.neo4j.fabric.eval.Catalog.ExternalGraph
import org.neo4j.fabric.eval.Catalog.InternalGraph
import org.neo4j.fabric.eval.DatabaseLookup
import org.neo4j.fabric.eval.StaticEvaluation
import org.neo4j.fabric.eval.UseEvaluation
import org.neo4j.fabric.pipeline.SignatureResolver
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.procedure.impl.GlobalProceduresRegistry
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.parboiled.scala.ReportingParseRunner
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable

class UseEvaluationTest
  extends FabricTest
    with ProcedureSignatureResolverTestSupport
    with TestName {

  private val mega0 = new Graph(0L, FabricEnterpriseConfig.RemoteUri.create("bolt://mega:1111"), "neo4j", new NormalizedGraphName("source_of_all_truth"), null)
  private val mega1 = new Graph(1L, FabricEnterpriseConfig.RemoteUri.create("bolt://mega:2222"), "neo4j", null, null)
  private val mega2 = new Graph(2L, FabricEnterpriseConfig.RemoteUri.create("bolt://mega:3333"), "neo4j", new NormalizedGraphName("mega"), null)

  private val neo4jUuid = UUID.randomUUID()
  private val testUuid = UUID.randomUUID()
  private val megaUuid = UUID.randomUUID()

  private val config = new FabricEnterpriseConfig(
    new FabricEnterpriseConfig.Database(new NormalizedDatabaseName("mega"), util.Set.of(mega0, mega1, mega2)),
    util.List.of(), Duration.ZERO, Duration.ZERO,
    new GlobalDriverConfig(Duration.ZERO, Duration.ZERO, 0, null),
    new FabricConfig.DataStream(300, 1000, 50, 10),
    false
  )

  private val internalDbs = Set(dbId("neo4j", neo4jUuid), dbId("test", testUuid), dbId("mega", megaUuid))

  "Correctly evaluates:" - {
    "USE mega.graph(0)" in eval().shouldEqual(external(mega0))
    "USE mega.graph(1)" in eval().shouldEqual(external(mega1))
    "USE mega.graph(const0())" in eval().shouldEqual(external(mega0))
    "USE mega.graph(const1())" in eval().shouldEqual(external(mega1))
    "USE mega.graph(x)" in eval("x" -> Values.intValue(0)).shouldEqual(external(mega0))
    "USE mega.graph(y)" in eval("y" -> Values.intValue(1)).shouldEqual(external(mega1))
    "USE mega.source_of_all_truth" in eval().shouldEqual(external(mega0))
    "USE mega.mega" in eval().shouldEqual(external(mega2))
    "USE meGA.Graph(0)" in eval().shouldEqual(external(mega0))
    "USE MEGA.GRAPH(1)" in eval().shouldEqual(external(mega1))
    "USE Mega.Graph(Const0())" in eval().shouldEqual(external(mega0))
    "USE MEGA.GRAPH(CONST1())" in eval().shouldEqual(external(mega1))
    "USE mega.Graph(x)" in eval("x" -> Values.intValue(0)).shouldEqual(external(mega0))
    "USE mega.GRAPH(y)" in eval("y" -> Values.intValue(1)).shouldEqual(external(mega1))
    "USE mega.sOuRce_Of_aLL_tRuTH" in eval().shouldEqual(external(mega0))
    "USE Mega.MEGA" in eval().shouldEqual(external(mega2))
    "USE mega" in eval().shouldEqual(internal(3, megaUuid, "mega"))
    "USE MeGa" in eval().shouldEqual(internal(3, megaUuid, "mega"))
    "USE neo4j" in eval().shouldEqual(internal(4, neo4jUuid, "neo4j"))
    "USE Neo4j" in eval().shouldEqual(internal(4, neo4jUuid, "neo4j"))
    "USE test" in eval().shouldEqual(internal(5, testUuid, "test"))
    "USE mega.graph(4)" in eval().shouldEqual(internal(4, neo4jUuid, "neo4j"))
  }

  "Fails for:" - {
    "USE mega.graph0" in the[EntityNotFoundException].thrownBy(eval()).getMessage.should(include("not found: mega.graph0"))
    "USE mega.graph1" in the[EntityNotFoundException].thrownBy(eval()).getMessage.should(include("not found: mega.graph1"))
    "USE mega.graph(10)" in the[EntityNotFoundException].thrownBy(eval()).getMessage.should(include("not found: 10"))
    "USE mega.graph('p')" in the[CypherTypeException].thrownBy(eval()).getMessage.should(include("Wrong type"))
    "USE mega.graph(1, 2)" in the[SyntaxException].thrownBy(eval()).getMessage.should(include("Wrong arity"))
    "USE mega.GRAph0" in the[EntityNotFoundException].thrownBy(eval()).getMessage.should(include("not found: mega.GRAph0"))
    "USE MEGA.graph1" in the[EntityNotFoundException].thrownBy(eval()).getMessage.should(include("not found: MEGA.graph1"))
    "USE foo" in the[EntityNotFoundException].thrownBy(eval()).getMessage.should(include("not found: foo"))
    "USE internal" in the[EntityNotFoundException].thrownBy(eval()).getMessage.should(include("not found: internal"))
  }

  object eval {

    private object parse extends Query with Clauses {
      def apply(use: String): UseGraph =
        ReportingParseRunner(this.UseGraph).run(use).result.get
    }

    private val databaseLookup = new DatabaseLookup {

      override def databaseIds: Set[NamedDatabaseId] = internalDbs

      override def databaseId(databaseName: NormalizedDatabaseName): Option[NamedDatabaseId] = ???
    }
    private val databaseManagementService = MockitoSugar.mock[DatabaseManagementService]
    private val catalogManager = new EnterpriseSingleCatalogManager(databaseLookup, databaseManagementService, config)
    private val catalog = catalogManager.currentCatalog()
    private val procedures = {
      val reg = new GlobalProceduresRegistry()
      callableProcedures.foreach(reg.register)
      callableUseFunctions.foreach(reg.register)
      reg
    }
    private val signatures = new SignatureResolver(() => procedures)
    private val staticEvaluator = new StaticEvaluation.StaticEvaluator(() => procedures)

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
    ): Catalog.Graph = {
      val evaluation = new UseEvaluation.Instance(use, catalog, staticEvaluator, signatures)
      evaluation.evaluate(parse(use), params, context.asJava)
    }
  }

  private def external(graph: FabricEnterpriseConfig.Graph) = ExternalGraph(graph.getId, Option(graph.getName).map(_.name()), new UUID(graph.getId, 0))
  private def internal(id: Long, uuid: UUID, name: String) = InternalGraph(id, uuid, new NormalizedGraphName(name), new NormalizedDatabaseName(name))
  private def dbId(name: String, uuid:UUID):NamedDatabaseId = DatabaseIdFactory.from(name, uuid)

}
