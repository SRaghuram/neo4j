/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.pipelined

import org.neo4j.cypher.internal.PipelinedRuntime.CODE_GEN_FAILED_MESSAGE
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.logging.AssertableLogProvider
import org.neo4j.logging.AssertableLogProvider.Level
import org.neo4j.logging.LogAssertions.assertThat
import org.scalatest.Exceptional
import org.scalatest.Outcome
import org.scalatest.TestSuiteMixin

/**
 * Assert that fusing succeeded as part of each test.
 */
trait AssertFusingSucceeded extends TestSuiteMixin {
  self: CypherFunSuite =>

  def logProvider: AssertableLogProvider

  abstract override def withFixture(test: NoArgTest): Outcome = {
    val result = super.withFixture(test)

    try {
      assertThat(logProvider).forLevel(Level.DEBUG).doesNotContainMessage(CODE_GEN_FAILED_MESSAGE)
      result
    } catch {
      case t: Throwable =>
        Exceptional(t)
    }
  }
}
