/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.pipelined

import java.nio.file.Files
import java.nio.file.Path

import org.neo4j.codegen.api.CodeGeneration.GENERATED_SOURCE_LOCATION_PROPERTY
import org.neo4j.codegen.api.CodeGeneration.GENERATE_JAVA_SOURCE_DEBUG_OPTION
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

trait PipelinedDebugGeneratedSourceEnabled extends PipelinedDebugGeneratedSource {
  self: RuntimeTestSuite[EnterpriseRuntimeContext] =>

  override val saveGeneratedSourceEnabled: Boolean = true
  override val keepSourceFilesAfterTestFinishes: Boolean = false
  override val logSaveLocation: Boolean = false // Do not spam test output

  override val debugOptions: Set[String] = Set(GENERATE_JAVA_SOURCE_DEBUG_OPTION)
}

abstract class PipelinedDebugTestBase(edition: Edition[EnterpriseRuntimeContext],
                                      runtime: CypherRuntime[EnterpriseRuntimeContext]
                                     ) extends RuntimeTestSuite[EnterpriseRuntimeContext](edition, runtime) with PipelinedDebugGeneratedSourceEnabled {

  test("should save generate source code") {
    val location = System.getProperty(GENERATED_SOURCE_LOCATION_PROPERTY)

    // preconditions
    saveGeneratedSourceEnabled shouldBe true
    location.nonEmpty shouldBe true
    Files.exists(Path.of(location).resolve("org/neo4j/codegen/OperatorPipeline0_0.java")) shouldBe false
    Files.exists(Path.of(location).resolve("org/neo4j/codegen/OperatorTaskPipeline0_0.java")) shouldBe false

    // run
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .allNodeScan("x")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // postconditions
    runtimeResult should beColumns("x").withNoRows()
    Files.exists(Path.of(location).resolve("org/neo4j/codegen/OperatorPipeline0_0.java")) shouldBe true
    Files.exists(Path.of(location).resolve("org/neo4j/codegen/OperatorTaskPipeline0_0.java")) shouldBe true
  }
}