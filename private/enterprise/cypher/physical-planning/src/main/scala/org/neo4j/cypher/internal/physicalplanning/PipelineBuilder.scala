/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.compiler.v4_0.planner.CantCompileQueryException
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.{ApplyPlans, SlotConfigurations}
import org.neo4j.cypher.internal.physicalplanning.PipelineBuilder.NO_PRODUCING_PIPELINE
import org.neo4j.cypher.internal.physicalplanning.SlotAllocation.PhysicalPlan
import org.neo4j.cypher.internal.v4_0.logical.plans
import org.neo4j.cypher.internal.v4_0.logical.plans._
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

import scala.collection.mutable.ArrayBuffer

// TODO: refactor this file into a proper builder pattern, where the result of
//  building is immutable definitions, and mutability is visible inside the builder only

class RowBufferDefinition(val id: Int,
                          val producingPipelineId: Int) {
  val counters = new ArrayBuffer[CounterDefinition]
}

class ArgumentBufferDefinition(id: Int,
                               producingPipelineId: Int,
                               applyPlanId: Id,
                               val argumentSlotOffset: Int) extends RowBufferDefinition(id, producingPipelineId)

case class CounterDefinition(reducingPlanId: Id, argumentSlotOffset: Int)

class Pipeline(val id: Int,
               val headPlan: LogicalPlan) {
  var output: RowBufferDefinition = _
  val middlePlans = new ArrayBuffer[LogicalPlan]
  var produceResults: Option[ProduceResult] = None
  var lhsRowBuffer: RowBufferDefinition = _
}

class StateDefinition(val physicalPlan: PhysicalPlan) {

  val counters = new ArrayBuffer[CounterDefinition]
  val rowBuffers = new ArrayBuffer[RowBufferDefinition]

  def newStreamingBuffer(producingPipelineId: Int): RowBufferDefinition = {
    val x = rowBuffers.size
    val rows = new RowBufferDefinition(x, producingPipelineId)
    rowBuffers += rows
    rows
  }

  def newArgumentBuffer(producingPipelineId: Int,
                        applyPlanId: Id,
                        argumentSlotOffset: Int): ArgumentBufferDefinition = {
    val x = rowBuffers.size
    val rows = new ArgumentBufferDefinition(x, producingPipelineId, applyPlanId, argumentSlotOffset)
    rowBuffers += rows
    rows
  }

  def newCounter(reducingPlanId: Id, argumentSlotOffset: Int): CounterDefinition = {
    val x = CounterDefinition(reducingPlanId, argumentSlotOffset)
    counters += x
    x
  }

  var initBuffer: ArgumentBufferDefinition = _
}

object PipelineBuilder {
  val NO_PRODUCING_PIPELINE: Int = -1
}

class PipelineBuilder(breakingPolicy: PipelineBreakingPolicy,
                      stateDefinition: StateDefinition,
                      slotConfigurations: SlotConfigurations)
  extends TreeBuilder2[Pipeline, ArgumentBufferDefinition] {

  val pipelines = new ArrayBuffer[Pipeline]

  private def newPipeline(plan: LogicalPlan) = {
    val pipeline = new Pipeline(pipelines.size, plan)
    pipelines += pipeline
    pipeline
  }

  private def outputToBuffer(pipeline: Pipeline): RowBufferDefinition = {
    val output = stateDefinition.newStreamingBuffer(pipeline.id)
    pipeline.output = output
    output
  }

  private def outputToArgumentBuffer(pipeline: Pipeline, applyPlanId: Id, argumentSlotOffset: Int): ArgumentBufferDefinition = {
    val output = stateDefinition.newArgumentBuffer(pipeline.id, applyPlanId, argumentSlotOffset)
    pipeline.output = output
    output
  }

  override protected def initialArgument(leftLeaf: LogicalPlan): ArgumentBufferDefinition = {
    val initialArgumentSlotOffset = slotConfigurations(leftLeaf.id).getArgumentLongOffsetFor(Id.INVALID_ID)
    stateDefinition.initBuffer = stateDefinition.newArgumentBuffer(NO_PRODUCING_PIPELINE, Id.INVALID_ID, initialArgumentSlotOffset)
    stateDefinition.initBuffer
  }

  override protected def onLeaf(plan: LogicalPlan,
                                argument: ArgumentBufferDefinition): Pipeline = {

    if (breakingPolicy.breakOn(plan)) {
      val pipeline = newPipeline(plan)
      pipeline.lhsRowBuffer = argument
      pipeline
    } else
      throw new UnsupportedOperationException("not implemented")
  }

  override protected def onOneChildPlan(plan: LogicalPlan,
                                        source: Pipeline,
                                        argument: ArgumentBufferDefinition): Pipeline = {
    plan match {
      case produceResult: ProduceResult =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          pipeline.lhsRowBuffer = outputToBuffer(source)
          pipeline.produceResults = Some(produceResult)
          pipeline
        } else {
          source.produceResults = Some(produceResult)
          source
        }

      case _: Sort =>
        if (breakingPolicy.breakOn(plan)) {
          source.middlePlans += plan

          val counter = stateDefinition.newCounter(plan.id, argument.argumentSlotOffset)
          val pipeline = newPipeline(plan)
          pipeline.lhsRowBuffer = outputToBuffer(source)
          forBuffersUntil(pipeline.lhsRowBuffer, argument, b => b.counters += counter)
          pipeline
        } else throw new UnsupportedOperationException("not implemented")

      case _: Expand |
           _: PruningVarExpand |
           _: VarExpand |
           _: OptionalExpand |
           _: FindShortestPaths |
           _: UnwindCollection |
           _: VarExpand =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          pipeline.lhsRowBuffer = outputToBuffer(source)
          pipeline
        } else {
          source.middlePlans += plan
          source
        }

      case _ =>
        source.middlePlans += plan
        source
    }
  }

  override protected def onTwoChildPlanComingFromLeft(plan: LogicalPlan,
                                                      lhs: Pipeline,
                                                      argument: ArgumentBufferDefinition): ArgumentBufferDefinition =
  {
    plan match {
      case _: plans.Apply =>
        val argumentSlotOffset = slotConfigurations(plan.id).getArgumentLongOffsetFor(plan.id)
        outputToArgumentBuffer(lhs, plan.id, argumentSlotOffset)

      case _ =>
        argument
    }
  }

  override protected def onTwoChildPlanComingFromRight(plan: LogicalPlan, lhs: Pipeline, rhs: Pipeline): Pipeline = {

    plan match {
      case _: plans.Apply =>
        rhs

      case p =>
        throw new CantCompileQueryException(s"$plan not supported in morsel runtime")
    }
  }

  // HELPERS

  private def forBuffersUntil(startBuffer: RowBufferDefinition,
                              endBuffer: RowBufferDefinition,
                              function: RowBufferDefinition => Unit): Unit = {
    var b = startBuffer
    while (b != endBuffer) {
      function(b)
      b = pipelines(b.producingPipelineId).lhsRowBuffer
    }
    function(b)
  }
}
