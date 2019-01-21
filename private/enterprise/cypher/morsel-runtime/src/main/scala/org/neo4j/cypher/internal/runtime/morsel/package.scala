/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime

package object morsel {
  /**
    * An argument that can be passed to a pipeline task that will be forwarded to downstream pipeline tasks
    * TBD: This could be merged into QueryState if we copy the QueryState at the appropriate points
    * which would resemble how we do things in the interpreted/slotted runtime
    */
  type SinglePARG = MorselExecutionContext
  type PipelineArgument = List[SinglePARG]
  val EmptyPipelineArgument: PipelineArgument = Nil
}
