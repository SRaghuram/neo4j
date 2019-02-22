/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.CompiledBytecode
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.CompiledSource
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.Interpreted
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.MorselSingleThreaded
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.Slotted
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.SlottedWithCompiledExpressions

object Configs {

  // Configurations with runtimes
  def Compiled: TestConfiguration = TestConfiguration(Planners.all, Runtimes(CompiledSource, CompiledBytecode))

  def Morsel: TestConfiguration = TestConfiguration(Planners.all, Runtimes(Runtimes.Morsel, MorselSingleThreaded))

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
      all - TestConfiguration(Planners.all, Runtimes(Runtimes.Morsel))
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
}
