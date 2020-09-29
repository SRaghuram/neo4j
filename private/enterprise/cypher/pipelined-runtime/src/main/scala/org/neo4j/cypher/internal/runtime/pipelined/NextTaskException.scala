/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatorAndPayload

case class NextTaskException(pipeline: ExecutablePipeline, cause: Throwable) extends Exception(cause)
case class SchedulingInputException(input: SchedulingInputException.Input, cause: Throwable) extends Exception(cause)

object SchedulingInputException {
  sealed trait Input
  case class MorselParallelizerInput(morsel: MorselParallelizer) extends Input
  case class MorselAccumulatorsInput(acc: IndexedSeq[MorselAccumulator[_ <: AnyRef]]) extends Input
  case class AccumulatorAndPayloadInput[ACC_DATA <: AnyRef, ACC <: MorselAccumulator[ACC_DATA], PAYLOAD <: AnyRef](accAndMorsel: AccumulatorAndPayload[ACC_DATA, ACC, PAYLOAD]) extends Input
  case class DataInput(data: AnyRef) extends Input
}
