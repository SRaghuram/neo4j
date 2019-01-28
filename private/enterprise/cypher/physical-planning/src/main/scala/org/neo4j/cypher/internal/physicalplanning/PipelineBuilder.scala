/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.compiler.v4_0.planner.CantCompileQueryException
import org.neo4j.cypher.internal.v4_0.logical.plans
import org.neo4j.cypher.internal.v4_0.logical.plans._

import scala.collection.mutable.ArrayBuffer

sealed trait Dependency
case class Rows(id: Int, all: Boolean) extends Dependency
case class ContinueRows(id: Int) extends Dependency
case class HashTable(id: Int) extends Dependency
case object Sink extends Dependency

class Pipeline(val id: Int,
               val headPlan: LogicalPlan,
               val serial: Boolean,
               needsArgumentCount: Boolean) {
  var output: Dependency = _
  val middlePlans = new ArrayBuffer[LogicalPlan]
  var produceResults: Option[ProduceResult] = None
  var lhsRows: Rows = _
  val rhsRows: Option[Rows] = None
  val dependencies = new ArrayBuffer[Dependency]
}

case class Buffer(id: Int)

class StateDefinition {

  var bufferCount = 0
  var hashTableCount = 0

  def newStreamingBuffer(): Rows = {
    val x = bufferCount
    bufferCount += 1
    Rows(x, false)
  }

  def newHashTable(): HashTable = {
    val x = hashTableCount
    hashTableCount += 1
    HashTable(x)
  }

  val initBuffer: Rows = newStreamingBuffer()
}

class PipelineBuilder(breakingPolicy: PipelineBreakingPolicy,
                      stateDefinition: StateDefinition)
  extends TreeBuilder[Pipeline] {

  val pipelines = new ArrayBuffer[Pipeline]

  private def openPipeline(plan: LogicalPlan, serial: Boolean, needsArgumentCount: Boolean): Pipeline = {
    val pipeline = new Pipeline(pipelines.size, plan, serial, needsArgumentCount)
    pipelines += pipeline
    pipeline
  }

  private def closePipeline[T <: Dependency](pipeline: Pipeline, output: T): T = {
    pipeline.output = output
    output
  }

  override protected def onLeaf(plan: LogicalPlan, sourceOpt: Option[Pipeline]): Pipeline = {
    val pipeline = openPipeline(plan, false, false)
    sourceOpt match {
      case None => pipeline.lhsRows = stateDefinition.initBuffer
      case Some(source) =>
        source.output match {
          case rows: Rows => pipeline.lhsRows = rows
          case _ => ???
        }
    }
    pipeline
  }

  override protected def onOneChildPlan(plan: LogicalPlan, source: Pipeline): Pipeline = {
    val id = plan.id

    plan match {
      case produceResult: ProduceResult =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = openPipeline(plan, true, false)
          pipeline.lhsRows = closePipeline(source, stateDefinition.newStreamingBuffer())
          pipeline.produceResults = Some(produceResult)
          pipeline.output = Sink
          pipeline
        } else {
          source.produceResults = Some(produceResult)
          source.output = Sink
          source
        }

      case _: Aggregation =>
        val pipeline = openPipeline(plan, false, true)
        pipeline.lhsRows = closePipeline(source, stateDefinition.newStreamingBuffer())
        pipeline

      case _: Sort if breakingPolicy.breakOn(plan) =>
        val buffer = stateDefinition.newStreamingBuffer()
        closePipeline(source, buffer)
        val pipeline = openPipeline(plan, false, true)
        // TODO: AllRows is probably not a good model because what happens on rhs?
        // TODO: Better to model like Aggr. and others like a in-operator group by argument
        pipeline.lhsRows = Rows(buffer.id, true)
        pipeline

      case _: Expand |
           _: PruningVarExpand |
           _: VarExpand |
           _: OptionalExpand |
           _: FindShortestPaths |
           _: UnwindCollection |
           _: VarExpand =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = openPipeline(plan, false, false)
          pipeline.lhsRows = closePipeline(source, stateDefinition.newStreamingBuffer())
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

  override protected def onTwoChildPlanComingFromLeft(plan: LogicalPlan, lhs: Pipeline): Option[Pipeline] =
  {
    plan match {
      case _: plans.Apply =>
        lhs.output = stateDefinition.newStreamingBuffer()
        Some(lhs)

      case _: plans.AbstractSemiApply =>
        lhs.output = stateDefinition.newStreamingBuffer()
        Some(lhs)

      case _ =>
        None
    }
  }

  override protected def onTwoChildPlanComingFromRight(plan: LogicalPlan, lhs: Pipeline, rhs: Pipeline): Pipeline = {

    plan match {
      case _: NodeHashJoin =>
        lhs.output = stateDefinition.newHashTable()
        val x =
          if (breakingPolicy.breakOn(plan)) {
            val pipeline = openPipeline(plan, false, false)
            pipeline.lhsRows = closePipeline(rhs, stateDefinition.newStreamingBuffer())
            pipeline
          } else
            rhs

        x.dependencies += lhs.output
        x

      case _: plans.Apply =>
        rhs

      case _: plans.AbstractSemiApply =>
        val pipeline = openPipeline(plan, false, false)
        pipeline.lhsRows = closePipeline(rhs, stateDefinition.newStreamingBuffer())
        pipeline

      case p =>
        throw new CantCompileQueryException(s"$plan not supported in morsel runtime")
    }
  }
}
