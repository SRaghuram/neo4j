/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.CypherRuntimeOption

object EnterpriseRuntimeFactory {

  private val MorselImpl = if (ZombieRuntime.ENABLED) ZombieRuntime else MorselRuntime

  val interpreted = new FallbackRuntime[EnterpriseRuntimeContext](List(InterpretedRuntime), CypherRuntimeOption.interpreted)
  val slotted = new FallbackRuntime[EnterpriseRuntimeContext](List(SlottedRuntime, InterpretedRuntime), CypherRuntimeOption.slotted)
  val compiledWithoutFallback = new FallbackRuntime[EnterpriseRuntimeContext](List(CompiledRuntime), CypherRuntimeOption.compiled)
  val compiled = new FallbackRuntime[EnterpriseRuntimeContext](List(CompiledRuntime, SlottedRuntime, InterpretedRuntime), CypherRuntimeOption.compiled)
  val morselWithoutFallback = new FallbackRuntime[EnterpriseRuntimeContext](List(MorselImpl), CypherRuntimeOption.morsel)
  val morsel = new FallbackRuntime[EnterpriseRuntimeContext](List(MorselImpl, CompiledRuntime, SlottedRuntime, InterpretedRuntime), CypherRuntimeOption.morsel)
  val default = new FallbackRuntime[EnterpriseRuntimeContext](List(CompiledRuntime, SlottedRuntime, InterpretedRuntime), CypherRuntimeOption.default)

  def getRuntime(cypherRuntime: CypherRuntimeOption, disallowFallback: Boolean): CypherRuntime[EnterpriseRuntimeContext] =
    cypherRuntime match {
      case CypherRuntimeOption.interpreted => interpreted

      case CypherRuntimeOption.slotted => slotted

      case CypherRuntimeOption.compiled if disallowFallback => compiledWithoutFallback

      case CypherRuntimeOption.compiled => compiled

      case CypherRuntimeOption.morsel if disallowFallback => morselWithoutFallback

      case CypherRuntimeOption.morsel => morsel

      case CypherRuntimeOption.default => default
    }
}
