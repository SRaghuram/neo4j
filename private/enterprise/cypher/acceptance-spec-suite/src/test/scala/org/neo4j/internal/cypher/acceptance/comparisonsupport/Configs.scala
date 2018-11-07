/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.CompiledBytecode
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.CompiledSource
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.Interpreted
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.Slotted
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.SlottedWithCompiledExpressions
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Versions.V3_4
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Versions.V4_0

object Configs {

  // Configurations with runtimes
  def Compiled: TestConfiguration = TestConfiguration(V3_4 -> V4_0, Planners.all, Runtimes(CompiledSource, CompiledBytecode))

  def Morsel: TestConfiguration = TestConfiguration(V3_4 -> V4_0, Planners.all, Runtimes(Runtimes.Morsel))

  def InterpretedRuntime: TestConfiguration =
    TestConfiguration(Versions.all, Planners.all, Runtimes(Interpreted))

  def SlottedRuntime: TestConfiguration = TestConfiguration(V3_4 -> V4_0, Planners.all, Runtimes(Slotted, SlottedWithCompiledExpressions))

  def InterpretedAndSlotted: TestConfiguration = InterpretedRuntime + SlottedRuntime

  // Configurations for versions
  def Version3_4: TestConfiguration = TestConfiguration(V3_4, Planners.all, Runtimes.all)

  def Version4_0: TestConfiguration = TestConfiguration(V4_0, Planners.all, Runtimes.all)

  /**
    * These are all configurations that will be executed even if not explicitly expected to succeed or fail.
    * Even if not explicitly requested, they are executed to check if they unexpectedly succeed to make sure that
    * test coverage is kept up-to-date with new features.
    */
  def All: TestConfiguration =
    TestConfiguration(Versions.all, Planners.all, Runtimes.all) -
  // TODO remove when 3.5ing
      // No slotted runtime with compiled expressions before 3.5
      TestConfiguration(V3_4, Planners.all, Runtimes(SlottedWithCompiledExpressions))

  /**
    * These experimental configurations will only be executed if you explicitly specify them in the test expectation.
    * I.e. there will be no check to see if they unexpectedly succeed on tests where they were not explicitly requested.
    */
  def Experimental: TestConfiguration =
  //TestConfiguration(Versions.Default, Planners.Default, Runtimes(Runtimes.Morsel))
    TestConfiguration.empty

  def Empty: TestConfiguration = TestConfiguration.empty

}
