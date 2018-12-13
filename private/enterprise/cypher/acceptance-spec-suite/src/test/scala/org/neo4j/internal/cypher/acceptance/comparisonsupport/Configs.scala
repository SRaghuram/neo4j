/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Versions.V3_5
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Versions.V4_0

object Configs {

  // Configurations with runtimes
  def Compiled: TestConfiguration = TestConfiguration(V3_5 -> V4_0, Planners.all, Runtimes(CompiledSource, CompiledBytecode))

  def Morsel: TestConfiguration = TestConfiguration(V3_5 -> V4_0, Planners.all, Runtimes(Runtimes.Morsel, MorselSingleThreaded))

  def InterpretedRuntime: TestConfiguration =
    TestConfiguration(Versions.all, Planners.all, Runtimes(Interpreted))

  def SlottedRuntime: TestConfiguration = TestConfiguration(V3_5 -> V4_0, Planners.all, Runtimes(Slotted, SlottedWithCompiledExpressions))

  def InterpretedAndSlotted: TestConfiguration = InterpretedRuntime + SlottedRuntime

  def InterpretedAndSlottedAndMorsel: TestConfiguration = InterpretedRuntime + SlottedRuntime + Morsel

  // Configurations for versions
  def Version3_5: TestConfiguration = TestConfiguration(V3_5, Planners.all, Runtimes.all)

  def Version4_0: TestConfiguration = TestConfiguration(V4_0, Planners.all, Runtimes.all)

  /**
    * These are all configurations that will be executed even if not explicitly expected to succeed or fail.
    * Even if not explicitly requested, they are executed to check if they unexpectedly succeed to make sure that
    * test coverage is kept up-to-date with new features.
    */
  def All: TestConfiguration = TestConfiguration(Versions.all, Planners.all, Runtimes.all)

  /**
    * These experimental configurations will only be executed if you explicitly specify them in the test expectation.
    * I.e. there will be no check to see if they unexpectedly succeed on tests where they were not explicitly requested.
    */
  def Experimental: TestConfiguration =
    TestConfiguration(Versions.V4_0, Planners.all, Runtimes(Runtimes.Morsel))

  def Empty: TestConfiguration = TestConfiguration.empty

}
