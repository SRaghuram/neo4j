/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.parallel

import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimitedTests}
import org.scalatest.time.{Minutes, Span}

/**
 * Limits tests in a class to 5 minutes.
 */
trait TimeLimitedCypherTest extends TimeLimitedTests {
  self: CypherFunSuite =>

  override val timeLimit = Span(5, Minutes)
  override val defaultTestSignaler: Signaler = ThreadSignaler
}
