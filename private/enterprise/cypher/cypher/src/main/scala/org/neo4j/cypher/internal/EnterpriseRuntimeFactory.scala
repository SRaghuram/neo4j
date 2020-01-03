/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.CypherRuntimeOption

object EnterpriseRuntimeFactory {

  val interpreted = new FallbackRuntime[EnterpriseRuntimeContext](List(SchemaCommandRuntime, InterpretedRuntime), CypherRuntimeOption.interpreted)
  val slotted = new FallbackRuntime[EnterpriseRuntimeContext](List(SchemaCommandRuntime, SlottedRuntime), CypherRuntimeOption.slotted)
  val compiledWithoutFallback = new FallbackRuntime[EnterpriseRuntimeContext](List(SchemaCommandRuntime, CompiledRuntime), CypherRuntimeOption.compiled)
  val compiled = new FallbackRuntime[EnterpriseRuntimeContext](List(SchemaCommandRuntime, CompiledRuntime, SlottedRuntime), CypherRuntimeOption.compiled)
  val pipelinedWithoutFallback = new FallbackRuntime[EnterpriseRuntimeContext](List(SchemaCommandRuntime, PipelinedRuntime.PIPELINED), CypherRuntimeOption.pipelined)
  val pipelined = new FallbackRuntime[EnterpriseRuntimeContext](List(SchemaCommandRuntime, PipelinedRuntime.PIPELINED, SlottedRuntime), CypherRuntimeOption.pipelined)
  val parallelWithoutFallback = new FallbackRuntime[EnterpriseRuntimeContext](List(PipelinedRuntime.PARALLEL), CypherRuntimeOption.parallel)
  val default = new FallbackRuntime[EnterpriseRuntimeContext](List(SchemaCommandRuntime, PipelinedRuntime.PIPELINED, SlottedRuntime), CypherRuntimeOption.default)

  def getRuntime(cypherRuntime: CypherRuntimeOption, disallowFallback: Boolean): CypherRuntime[EnterpriseRuntimeContext] =
    cypherRuntime match {
      case CypherRuntimeOption.interpreted => interpreted

      case CypherRuntimeOption.slotted => slotted

      case CypherRuntimeOption.compiled if disallowFallback => compiledWithoutFallback

      case CypherRuntimeOption.compiled => compiled

      case CypherRuntimeOption.`pipelined` if disallowFallback => pipelinedWithoutFallback

      case CypherRuntimeOption.`pipelined` => pipelined

      case CypherRuntimeOption.default => default

      case CypherRuntimeOption.parallel => parallelWithoutFallback
    }
}
