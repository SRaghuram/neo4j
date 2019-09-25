/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.{CompiledBytecode, CompiledSource, Interpreted, Slotted, SlottedWithCompiledExpressions}

object Configs {

  // Configurations with runtimes
  def Compiled: TestConfiguration = TestConfiguration(Planners.all, Runtimes(CompiledSource, CompiledBytecode))

  def MorselSingleThreaded: TestConfiguration = TestConfiguration(Planners.all, Runtimes(Runtimes.Morsel))

  def Morsel: TestConfiguration = TestConfiguration(Planners.all, Runtimes(Runtimes.Parallel, Runtimes.Morsel))

  def Parallel: TestConfiguration = TestConfiguration(Planners.all, Runtimes(Runtimes.Parallel))

  def InterpretedRuntime: TestConfiguration = TestConfiguration(Planners.all, Runtimes(Interpreted))

  def SlottedRuntime: TestConfiguration = TestConfiguration(Planners.all, Runtimes(Slotted, SlottedWithCompiledExpressions))

  def InterpretedAndSlotted: TestConfiguration = InterpretedRuntime + SlottedRuntime

  def InterpretedAndSlottedAndMorsel: TestConfiguration = InterpretedRuntime + SlottedRuntime + Morsel

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
  def Experimental: TestConfiguration = TestConfiguration.empty

  def Empty: TestConfiguration = TestConfiguration.empty

  def runOnlySafeScenarios: Boolean = {
    val runExperimental = System.getenv().containsKey("RUN_EXPERIMENTAL")
    !runExperimental
  }

  assert((All /\ Experimental) == Empty, s"No experimental scenario should exist in any other test configuration, but these are: ${All /\ Experimental}")

  // The below test-configurations map to operators and constructs that stopped test
  // from being supported in morsel. When adding support for one of the below in morsel,
  // adding it here should be a fast way to correct many tests, although some might still
  // fail for lack of some other operator or construct.
  val NodeById              : TestConfiguration = InterpretedRuntime + SlottedRuntime + Compiled + Morsel
  val RelationshipById      : TestConfiguration = InterpretedRuntime + SlottedRuntime + Morsel
  val NodeIndexEndsWithScan : TestConfiguration = InterpretedRuntime + SlottedRuntime + Morsel
  val CartesianProduct      : TestConfiguration = InterpretedRuntime + SlottedRuntime + Compiled
  val ShortestPath          : TestConfiguration = InterpretedRuntime + SlottedRuntime + Morsel
  val OptionalExpand        : TestConfiguration = InterpretedRuntime + SlottedRuntime + Morsel
  val Optional              : TestConfiguration = InterpretedRuntime + SlottedRuntime + Morsel
  val RollUpApply           : TestConfiguration = InterpretedRuntime + SlottedRuntime
  val VarExpand             : TestConfiguration = InterpretedRuntime + SlottedRuntime + Morsel
  val ExpandInto            : TestConfiguration = InterpretedRuntime + SlottedRuntime + Compiled + Morsel
  val DropResult            : TestConfiguration = InterpretedRuntime + SlottedRuntime + Morsel
  val FromCountStore        : TestConfiguration = InterpretedRuntime + SlottedRuntime + Compiled + Morsel
  val UDF                   : TestConfiguration = InterpretedRuntime + SlottedRuntime + MorselSingleThreaded
  val CachedProperty        : TestConfiguration = InterpretedRuntime + SlottedRuntime + Morsel
  val NestedPlan            : TestConfiguration = InterpretedRuntime + SlottedRuntime
  val Create                : TestConfiguration = InterpretedRuntime + SlottedRuntime
  val ProcedureCall         : TestConfiguration = InterpretedRuntime + SlottedRuntime + MorselSingleThreaded
}
