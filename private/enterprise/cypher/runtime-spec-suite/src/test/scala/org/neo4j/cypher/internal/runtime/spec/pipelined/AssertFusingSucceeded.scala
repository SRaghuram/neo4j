/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.pipelined

import org.hamcrest.Matcher
import org.hamcrest.Matchers.instanceOf
import org.neo4j.cypher.internal.PipelinedRuntime
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.logging.AssertableLogProvider
import org.neo4j.logging.AssertableLogProvider.Level
import org.neo4j.logging.LogAssertions.assertThat
import org.scalatest.{Exceptional, Outcome, TestSuiteMixin}

/**
 * Assert that fusing succeeded as part of each test.
 */
trait AssertFusingSucceeded extends TestSuiteMixin {
  self: CypherFunSuite =>

  def logProvider: AssertableLogProvider

  abstract override def withFixture(test: NoArgTest): Outcome = {
    val result = super.withFixture(test)

    try {
      assertThat(logProvider).forLevel(Level.DEBUG).doesNotContainMessageWithArguments(
        PipelinedRuntime.CODE_GEN_FAILED_MESSAGE,
        instanceOf(classOf[Exception]): Matcher[Throwable] /*Type annotation is a compiler help to choose the right overloaded method - do not remove!*/)
      result
    } catch {
      case t: Throwable =>
        Exceptional(t)
    }
  }
}
