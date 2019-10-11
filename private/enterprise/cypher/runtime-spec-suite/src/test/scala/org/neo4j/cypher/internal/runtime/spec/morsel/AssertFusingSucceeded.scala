/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.hamcrest.CoreMatchers.any
import org.hamcrest.Matcher
import org.hamcrest.Matchers.{containsString, instanceOf}
import org.neo4j.cypher.internal.MorselRuntime
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.logging.AssertableLogProvider
import org.neo4j.logging.AssertableLogProvider.inLog
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
      logProvider.assertNone(inLog(any(classOf[String])).debug(
        containsString(MorselRuntime.CODE_GEN_FAILED_MESSAGE),
        instanceOf(classOf[CantCompileQueryException]): Matcher[Throwable] /*Type annotation is a compiler help to choose the right overloaded method - do not remove!*/))
      result
    } catch {
      case t: Throwable =>
        Exceptional(t)
    }
  }
}
