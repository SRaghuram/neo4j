/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.pipelined

import java.nio.file.FileVisitResult
import java.nio.file.FileVisitResult.CONTINUE
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes

import org.neo4j.codegen.api.CodeGeneration.GENERATED_SOURCE_LOCATION_PROPERTY
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.options.CypherDebugOption
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

trait PipelinedDebugGeneratedSourceEnabled extends PipelinedDebugGeneratedSource {
  self: RuntimeTestSuite[EnterpriseRuntimeContext] =>

  override val saveGeneratedSourceEnabled: Boolean = true
  override val keepSourceFilesAfterTestFinishes: Boolean = false
  override val logSaveLocation: Boolean = false // Do not spam test output

  override val debugOptions: CypherDebugOptions =
    CypherDebugOptions.default.withOptionEnabled(CypherDebugOption.generateJavaSource)
}

abstract class PipelinedDebugTestBase(edition: Edition[EnterpriseRuntimeContext],
                                      runtime: CypherRuntime[EnterpriseRuntimeContext]
                                     ) extends RuntimeTestSuite[EnterpriseRuntimeContext](edition, runtime) with PipelinedDebugGeneratedSourceEnabled {

  test("should save generate source code") {
    val location = System.getProperty(GENERATED_SOURCE_LOCATION_PROPERTY)

    // preconditions
    saveGeneratedSourceEnabled shouldBe true
    location.nonEmpty shouldBe true
    assertGeneratedSourceFiles(location, shouldExist = false)

    // run
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .allNodeScan("x")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // postconditions
    runtimeResult should beColumns("x").withNoRows()
    assertGeneratedSourceFiles(location, shouldExist = true)
  }

  private def assertGeneratedSourceFiles(location: String, shouldExist: Boolean): Unit = {
    val foundOperatorFile = "found operator java file"
    val foundOperatorTaskFile = "found operator task java file"
    val notFoundString = "not found"

    val (expectedHasOperatorFile, expectedHasOperatorTaskFile) =
      if (shouldExist) {
        (foundOperatorFile, foundOperatorTaskFile)
      } else {
        (notFoundString, notFoundString)
      }

    var hasOperatorFile = notFoundString
    var hasOperatorTaskFile = notFoundString

    val codeGenPath = Path.of(location).resolve("org/neo4j/codegen/")
    if (Files.exists(codeGenPath)) {
      Files.walkFileTree(codeGenPath, new SimpleFileVisitor[Path] {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          val fileName = file.getName(file.getNameCount - 1).toString
          // The second number in the filename is a global count of generated classes, so it varies with the number of tests that ran previously
          if (fileName.startsWith("OperatorPipeline0_") && fileName.endsWith(".java")) {
            hasOperatorFile = foundOperatorFile
          } else if (fileName.startsWith("OperatorTaskPipeline0_") && fileName.endsWith(".java")) {
            hasOperatorTaskFile = foundOperatorTaskFile
          }
          CONTINUE
        }
      })
    }

    hasOperatorFile shouldEqual expectedHasOperatorFile
    hasOperatorTaskFile shouldEqual expectedHasOperatorTaskFile
  }
}
