/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.Interpreted
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.SlottedWithCompiledExpressions
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.SlottedWithInterpretedExpressions

object Configs {

  // Configurations with runtimes
  def PipelinedSingleThreaded: TestConfiguration = TestConfiguration(Planners.all, Runtimes(Runtimes.PipelinedFused, Runtimes.PipelinedNonFused))

  def Pipelined: TestConfiguration = TestConfiguration(Planners.all, Runtimes(Runtimes.Parallel, Runtimes.PipelinedFused, Runtimes.PipelinedNonFused))

  def Parallel: TestConfiguration = TestConfiguration(Planners.all, Runtimes(Runtimes.Parallel))

  def InterpretedRuntime: TestConfiguration = TestConfiguration(Planners.all, Runtimes(Interpreted))

  def SlottedRuntime: TestConfiguration = TestConfiguration(Planners.all, Runtimes(SlottedWithInterpretedExpressions, SlottedWithCompiledExpressions))

  def InterpretedAndSlotted: TestConfiguration = InterpretedRuntime + SlottedRuntime

  def InterpretedAndSlottedAndPipelined: TestConfiguration = InterpretedRuntime + SlottedRuntime + Pipelined

  def PipelinedSingleThreadedFull: TestConfiguration = TestConfiguration(Planners.all, Runtimes(Runtimes.PipelinedFull))

  /**
   * These are all configurations that will be executed even if not explicitly expected to succeed or fail.
   * Even if not explicitly requested, they are executed to check if they unexpectedly succeed to make sure that
   * test coverage is kept up-to-date with new features.
   */
  def All: TestConfiguration = {
    val all = TestConfiguration(Planners.all, Runtimes.all)
    if (runOnlySafeScenarios) {
      all - TestConfiguration(Planners.all, Runtimes(Runtimes.Parallel))
    } else {
      all
    }
  }

  /**
   * These experimental configurations will only be executed if you explicitly specify them in the test expectation.
   * I.e. there will be no check to see if they unexpectedly succeed on tests where they were not explicitly requested.
   */
  def Experimental: TestConfiguration = PipelinedSingleThreadedFull

  def Empty: TestConfiguration = TestConfiguration.empty

  def runOnlySafeScenarios: Boolean = {
    val runExperimental = System.getenv().containsKey("RUN_EXPERIMENTAL")
    !runExperimental
  }

  assert((All /\ Experimental) == Empty, s"No experimental scenario should exist in any other test configuration, but these are: ${All /\ Experimental}")

  // The below test-configurations map to operators and constructs that stopped test
  // from being supported in pipelined. When adding support for one of the below in morsel,
  // adding it here should be a fast way to correct many tests, although some might still
  // fail for lack of some other operator or construct.
  val Create                : TestConfiguration = InterpretedRuntime + SlottedRuntime + Pipelined
  val ForEachApply          : TestConfiguration = InterpretedRuntime + SlottedRuntime
  val ProcedureCallWrite    : TestConfiguration = InterpretedRuntime + SlottedRuntime
  val UDF                   : TestConfiguration = InterpretedRuntime + SlottedRuntime + PipelinedSingleThreaded
  val NestedPlan            : TestConfiguration = InterpretedRuntime + SlottedRuntime + PipelinedSingleThreaded
  val ProcedureCallRead     : TestConfiguration = InterpretedRuntime + SlottedRuntime + PipelinedSingleThreaded
}
