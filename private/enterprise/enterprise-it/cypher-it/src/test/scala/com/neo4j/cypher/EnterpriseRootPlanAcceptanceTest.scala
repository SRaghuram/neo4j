/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cypher

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.InterpretedRuntimeName
import org.neo4j.cypher.internal.PipelinedRuntimeName
import org.neo4j.cypher.internal.RuntimeName
import org.neo4j.cypher.internal.SlottedRuntimeName
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.options.CypherVersion
import org.neo4j.cypher.internal.planner.spi.CostBasedPlannerName
import org.neo4j.cypher.internal.planner.spi.DPPlannerName
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.graphdb.ExecutionPlanDescription

import scala.collection.JavaConverters.mapAsScalaMapConverter

class EnterpriseRootPlanAcceptanceTest extends ExecutionEngineFunSuite with EnterpriseGraphDatabaseTestSupport {

  test("query that does not go through the pipelined runtime") {
    given("CREATE ()")
      .withCypherVersion(CypherVersion.default)
      .shouldHaveCypherVersion(CypherVersion.default)
      .shouldHaveRuntime(SlottedRuntimeName)
  }

  test("query that does not go through the pipelined runtime (explicit)") {
    given("CREATE ()")
      .withCypherVersion(CypherVersion.default)
      .withRuntime(PipelinedRuntimeName)
      .shouldHaveCypherVersion(CypherVersion.default)
      .shouldHaveRuntime(SlottedRuntimeName)
  }

  test("query that should go through the pipelined runtime") {
    given("MATCH (a)-->(b) RETURN a")
      .withCypherVersion(CypherVersion.default)
      .withRuntime(PipelinedRuntimeName)
      .shouldHaveCypherVersion(CypherVersion.default)
      .shouldHaveRuntime(PipelinedRuntimeName)
      .shouldHavePlanner(CostBasedPlannerName.default)
  }

  test("Rows should be properly formatted in pipelined runtime") {
    given("match (n) return n")
      .withRuntime(PipelinedRuntimeName)
      .planDescription.getArguments.get("Rows") should equal(0)
  }

  for(planner <- Seq(IDPPlannerName, DPPlannerName);
      runtime <- Seq(PipelinedRuntimeName, InterpretedRuntimeName)) {

    test(s"Should report correct planner and runtime used $planner + $runtime") {
      given("match (n) return n")
        .withPlanner(planner)
        .withRuntime(runtime)
        .shouldHaveCypherVersion(CypherVersion.default)
        .shouldHavePlanner(planner)
        .shouldHaveRuntime(runtime)
    }
  }

  test("should show_java_source for pipelined fused operators") {
    graph.withTx { tx =>
      val res = executeOfficial( tx,
        """CYPHER runtime=pipelined operatorEngine=compiled debug=generate_java_source debug=show_java_source
          |MATCH (n) RETURN n""".stripMargin)
      res.resultAsString()
      shouldContainSourceCode(res.getExecutionPlanDescription)
    }
  }

  test("should show_java_source for pipelined compiled expressions") {
    graph.withTx { tx =>
      val res = executeOfficial( tx,
        """CYPHER runtime=pipelined operatorEngine=interpreted expressionEngine=compiled debug=generate_java_source debug=show_java_source
          |MATCH (n) WHERE n.prop / 2 = 0 RETURN n""".stripMargin)
      res.resultAsString()
      shouldContainSourceCode(res.getExecutionPlanDescription)
    }
  }

  test("should show_java_source for slotted compiled expressions") {
    graph.withTx { tx =>
      val res = executeOfficial( tx,
        """CYPHER runtime=slotted expressionEngine=compiled debug=generate_java_source debug=show_java_source
          |MATCH (n) WHERE n.prop / 2 = 0 RETURN n""".stripMargin)
      res.resultAsString()
      shouldContainSourceCode(res.getExecutionPlanDescription)
    }
  }

  test("should show_bytecode for pipelined fused operators") {
    graph.withTx { tx =>
      val res = executeOfficial( tx,
        """CYPHER runtime=pipelined operatorEngine=compiled debug=show_bytecode
          |MATCH (n) RETURN n""".stripMargin)
      res.resultAsString()
      shouldContainByteCode(res.getExecutionPlanDescription)
    }
  }

  test("should show_bytecode for pipelined compiled expressions") {
    graph.withTx { tx =>
      val res = executeOfficial( tx,
        """CYPHER runtime=pipelined operatorEngine=interpreted expressionEngine=compiled debug=show_bytecode
          |MATCH (n) WHERE n.prop / 2 = 0 RETURN n""".stripMargin)
      res.resultAsString()
      shouldContainByteCode(res.getExecutionPlanDescription)
    }
  }

  test("should show_bytecode for slotted compiled expressions") {
    graph.withTx { tx =>
      val res = executeOfficial( tx,
        """CYPHER runtime=slotted expressionEngine=compiled debug=show_bytecode
          |MATCH (n) WHERE n.prop / 2 = 0 RETURN n""".stripMargin)
      res.resultAsString()
      shouldContainByteCode(res.getExecutionPlanDescription)
    }
  }

  test("should show_java_source and show_bytecode for pipelined fused operators") {
    graph.withTx { tx =>
      val res = executeOfficial( tx,
        """CYPHER runtime=pipelined operatorEngine=compiled debug=generate_java_source debug=show_java_source debug=show_bytecode
          |MATCH (n) RETURN n""".stripMargin)
      res.resultAsString()
      shouldContainByteCode(res.getExecutionPlanDescription)
    }
  }

  test("should show_java_source and show_bytecode for pipelined compiled expressions") {
    graph.withTx { tx =>
      val res = executeOfficial( tx,
        """CYPHER runtime=pipelined operatorEngine=interpreted expressionEngine=compiled debug=generate_java_source debug=show_java_source debug=show_bytecode
          |MATCH (n) WHERE n.prop / 2 = 0 RETURN n""".stripMargin)
      res.resultAsString()
      shouldContainByteCode(res.getExecutionPlanDescription)
    }
  }

  test("should show_java_source and show_bytecode for slotted compiled expressions") {
    graph.withTx { tx =>
      val res = executeOfficial( tx,
        """CYPHER runtime=slotted expressionEngine=compiled debug=generate_java_source debug=show_java_source debug=show_bytecode
          |MATCH (n) WHERE n.prop / 2 = 0 RETURN n""".stripMargin)
      res.resultAsString()
      shouldContainByteCode(res.getExecutionPlanDescription)
    }
  }

  private def shouldContainSourceCode(planDescription: ExecutionPlanDescription): Unit = {
    shouldContain("source", planDescription)
  }

  private def shouldContainByteCode(planDescription: ExecutionPlanDescription): Unit = {
    shouldContain("bytecode", planDescription)
  }


  private def shouldContain(argument:String, planDescription: ExecutionPlanDescription): Unit = {
    if(!planDescription.getArguments.asScala.exists {
      case (name: String, code: String) if name.startsWith(s"$argument:") =>
        !code.isEmpty
      case _ => false
    }) {
      fail(s"no $argument present: $planDescription")
    }
  }

  def given(query: String) = TestQuery(query)

  case class TestQuery(query: String,
                       cypherVersion: Option[CypherVersion] = None,
                       planner: Option[PlannerName] = None,
                       runtime: Option[RuntimeName] = None) {

    lazy val planDescription: ExecutionPlanDescription = execute()

    def withCypherVersion(version: CypherVersion): TestQuery = copy(cypherVersion = Some(version))

    def withPlanner(planner: PlannerName): TestQuery = copy(planner = Some(planner))

    def withRuntime(runtime: RuntimeName): TestQuery = copy(runtime = Some(runtime))

    def shouldHaveCypherVersion(version: CypherVersion): TestQuery = {
      planDescription.getArguments.get("version") should equal(s"CYPHER ${version.name}")
      this
    }

    def shouldHavePlanner(planner: PlannerName): TestQuery = {
      planDescription.getArguments.get("planner") should equal(s"${planner.toTextOutput}")
      planDescription.getArguments.get("planner-impl") should equal(s"${planner.name}")
      this
    }

    def shouldHaveRuntime(runtime: RuntimeName): TestQuery = {
      planDescription.getArguments.get("runtime") should equal(s"${runtime.toTextOutput}")
      planDescription.getArguments.get("runtime-impl") should equal(s"${runtime.name}")
      this
    }

    private def execute() = {
      val prepend = (cypherVersion, planner, runtime) match {
        case (None, None, None) => ""
        case _ =>
          val version = cypherVersion.map(_.name).getOrElse("")
          val plannerString = planner.map("planner=" + _.name).getOrElse("")
          val runtimeString = runtime.map("runtime=" + _.name).getOrElse("")
          s"CYPHER $version $plannerString $runtimeString"
      }
      graph.withTx { tx =>
        val result = executeOfficial( tx, s"$prepend PROFILE $query")
        result.resultAsString()
        result.getExecutionPlanDescription
      }
    }
  }
}
