/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.logical.plans.{LogicalPlan, ProduceResult}
import org.neo4j.cypher.internal.physicalplanning
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.words.MatcherWords

import scala.collection.mutable

object ExecutionGraphDefinitionMatcher {

  /**
    * @return a new [[ExecutionGraphDefinitionMatcher]]
    */
  def newGraph: ExecutionGraphDefinitionMatcher = new ExecutionGraphDefinitionMatcher()

  /**
    * Start defining a sequence of buffers and pipelines with this method.
    *
    *
    * The first time you specify a buffer, pipeline, reducer or canceller with a particular ID,
    * you have to supply also the default arguments. This applies to all functions that are reachable from here.
    *
    */
  def start(m: ExecutionGraphDefinitionMatcher) = new m.StartSequence()

  /**
    * Used for the syntax
    * ```
    * ... should plan {
    *
    * }
    * ```
    *
    */
  def plan(m: ExecutionGraphDefinitionMatcher): ExecutionGraphDefinitionMatcher = m
}

/**
  * Matcher for ExecutionGraphDefinitions that lets one build the expected graph
  * through a sequence of method calls that describe the connections between buffers and pipelines.
  */
class ExecutionGraphDefinitionMatcher() extends Matcher[ExecutionGraphDefinition] {

  private case class MatchablePipeline(id: PipelineId,
                                       plans: Seq[Class[_ <: LogicalPlan]],
                                       inputBuffer: BufferId,
                                       outputDefinition: MatchableOutputDefinition,
                                       serial: Boolean)

  private sealed trait MatchableOutputDefinition

  private object ProduceResultOutput extends MatchableOutputDefinition

  private case class MorselBufferOutput(id: BufferId) extends MatchableOutputDefinition

  private case class MorselArgumentStateBufferOutput(id: BufferId, argumentSlotOffset: Int) extends MatchableOutputDefinition

  private case class ReduceOutput(bufferId: BufferId) extends MatchableOutputDefinition

  private case object NoOutput extends MatchableOutputDefinition

  private val buffers = mutable.Map[Int, BufferDefinition]()
  private val pipelines = mutable.Map[Int, MatchablePipeline]()
  private val argumentStates = mutable.Map[Int, ArgumentStateDefinition]()

  private def out(outputDefinition: OutputDefinition): MatchableOutputDefinition = {
    outputDefinition match {
      case physicalplanning.ProduceResultOutput(_) => ProduceResultOutput
      case physicalplanning.MorselBufferOutput(id) => MorselBufferOutput(id)
      case physicalplanning.MorselArgumentStateBufferOutput(id, argumentSlotOffset) => MorselArgumentStateBufferOutput(id, argumentSlotOffset)
      case physicalplanning.ReduceOutput(bufferId, _) => ReduceOutput(bufferId)
      case physicalplanning.NoOutput => NoOutput
    }
  }

  private def produceResult(outputDefinition: OutputDefinition): Option[ProduceResult] = {
    outputDefinition match {
      case physicalplanning.ProduceResultOutput(plan) => Some(plan)
      case _ => None
    }
  }

  private def registerArgumentState(id: Int, planId: Int, argumentSlotOffset: Int): ArgumentStateDefinition = {
    argumentStates.getOrElseUpdate(id, ArgumentStateDefinition(ArgumentStateMapId(id), Id(planId), argumentSlotOffset))
  }

  class StartSequence {

    def applyBuffer(id: Int, argumentSlotOffset: Int = -1): ApplyBufferSequence = {
      val bd = buffers.getOrElseUpdate(id, ApplyBufferDefinition(BufferId(id), argumentSlotOffset, IndexedSeq.empty, IndexedSeq.empty, IndexedSeq.empty, IndexedSeq.empty)).asInstanceOf[ApplyBufferDefinition]
      new ApplyBufferSequence(bd)
    }

    def morselBuffer(id: Int): MorselBufferSequence = {
      // This buffer must have been specified before
      new MorselBufferSequence(buffers(id).asInstanceOf[MorselBufferDefinition])
    }
  }

  abstract class BufferSequence(bufferDefinition: BufferDefinition) {
    def reducer(id: Int, planId: Int = -1, argumentSlotOffset: Int = -1): ExecutionGraphDefinitionMatcher = {
      val asd = registerArgumentState(id, planId, argumentSlotOffset)
      buffers.put(bufferDefinition.id.x, bufferDefinition.withReducers(bufferDefinition.reducers :+ asd.id))
      ExecutionGraphDefinitionMatcher.this
    }

    def canceller(id: Int, planId: Int = -1, argumentSlotOffset: Int = -1): ExecutionGraphDefinitionMatcher = {
      val asd = registerArgumentState(id, planId, argumentSlotOffset)
      buffers.put(bufferDefinition.id.x, bufferDefinition.withWorkCancellers(bufferDefinition.workCancellers :+ asd))
      ExecutionGraphDefinitionMatcher.this
    }
  }

  class ApplyBufferSequence(applyBufferDefinition: ApplyBufferDefinition) extends BufferSequence(applyBufferDefinition) {

    def delegateToMorselBuffer(id: Int): MorselBufferSequence = {
      val bd = buffers.getOrElseUpdate(id, MorselBufferDefinition(BufferId(id), IndexedSeq.empty, IndexedSeq.empty)).asInstanceOf[MorselBufferDefinition]
      buffers(applyBufferDefinition.id.x) = applyBufferDefinition.copy(delegates = applyBufferDefinition.delegates :+ bd.id)
      new MorselBufferSequence(bd)
    }

    def reducerOnRHS(id: Int, planId: Int = -1, argumentSlotOffset: Int = -1): ExecutionGraphDefinitionMatcher = {
      val asd = registerArgumentState(id, planId, argumentSlotOffset)
      buffers.put(applyBufferDefinition.id.x, applyBufferDefinition.copy(reducersOnRHS = applyBufferDefinition.reducersOnRHS :+ asd))
      ExecutionGraphDefinitionMatcher.this
    }
  }

  abstract class BufferBeforePipelineSequence(bufferDefinition: BufferDefinition) extends BufferSequence(bufferDefinition) {
    def pipeline(id: Int, plans: Seq[Class[_ <: LogicalPlan]] = Seq.empty, serial: Boolean = false): PipelineSequence = {
      val mp = pipelines.getOrElseUpdate(id, MatchablePipeline(PipelineId(id), plans, bufferDefinition.id, null, serial))
      new PipelineSequence(mp)
    }

  }

  class MorselBufferSequence(morselBufferDefinition: MorselBufferDefinition) extends BufferBeforePipelineSequence(morselBufferDefinition)

  class ArgumentStateBufferSequence(argumentStateBufferDefinition: ArgumentStateBufferDefinition) extends BufferBeforePipelineSequence(argumentStateBufferDefinition)

  class JoinBufferSequence(joinBufferDefinition: LHSAccumulatingRHSStreamingBufferDefinition) extends BufferBeforePipelineSequence(joinBufferDefinition)

  class PipelineSequence(matchablePipeline: MatchablePipeline) {
    def argumentStateBuffer(id: Int, asmId: Int = -1): ArgumentStateBufferSequence = {
      val bd = buffers.getOrElseUpdate(id, ArgumentStateBufferDefinition(BufferId(id), ArgumentStateMapId(asmId), IndexedSeq.empty, IndexedSeq.empty)).asInstanceOf[ArgumentStateBufferDefinition]
      val out = ReduceOutput(BufferId(id))
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      new ArgumentStateBufferSequence(bd)
    }

    def applyBuffer(id: Int, argumentSlotOffset: Int = -1): ApplyBufferSequence = {
      val bd = buffers.getOrElseUpdate(id, ApplyBufferDefinition(BufferId(id), argumentSlotOffset, IndexedSeq.empty, IndexedSeq.empty, IndexedSeq.empty, IndexedSeq.empty)).asInstanceOf[ApplyBufferDefinition]
      val out = MorselBufferOutput(BufferId(id))
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      new ApplyBufferSequence(bd)
    }

    def leftOfJoinBuffer(id: Int, argumentSlotOffset: Int, lhsAsmId: Int = -1, rhsAsmId: Int = -1): JoinBufferSequence = {
      val out = MorselArgumentStateBufferOutput(BufferId(id), argumentSlotOffset)
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      val bd = buffers.getOrElseUpdate(id, LHSAccumulatingRHSStreamingBufferDefinition(BufferId(id), PipelineId(-1), PipelineId(-1), ArgumentStateMapId(lhsAsmId), ArgumentStateMapId(rhsAsmId), IndexedSeq.empty, IndexedSeq.empty)).asInstanceOf[LHSAccumulatingRHSStreamingBufferDefinition]
      val updatedBD = bd.copy(lhsPipelineId = matchablePipeline.id)
      // Update lhs pipeline ID
      buffers.update(id, updatedBD)
      new JoinBufferSequence(updatedBD)
    }

    def rightOfJoinBuffer(id: Int, argumentSlotOffset: Int, lhsAsmId: Int = -1, rhsAsmId: Int = -1): JoinBufferSequence = {
      val out = MorselArgumentStateBufferOutput(BufferId(id), argumentSlotOffset)
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      val bd = buffers.getOrElseUpdate(id, LHSAccumulatingRHSStreamingBufferDefinition(BufferId(id), PipelineId(-1), PipelineId(-1), ArgumentStateMapId(lhsAsmId), ArgumentStateMapId(rhsAsmId), IndexedSeq.empty, IndexedSeq.empty)).asInstanceOf[LHSAccumulatingRHSStreamingBufferDefinition]
      val updatedBD = bd.copy(rhsPipelineId = matchablePipeline.id)
      // Update rhs pipeline ID
      buffers.update(id, updatedBD)
      new JoinBufferSequence(updatedBD)
    }

    def end: ExecutionGraphDefinitionMatcher = {
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = ProduceResultOutput)
      ExecutionGraphDefinitionMatcher.this
    }
  }

  override def apply(graph: ExecutionGraphDefinition): MatchResult = {
    val expectedBuffers = buffers.values.toSeq.sortBy(_.id.x)
    val gotBuffers = graph.buffers
    if (expectedBuffers != gotBuffers) {
      return MatcherWords.equal(expectedBuffers).matcher[Seq[BufferDefinition]].apply(gotBuffers)
    }

    val expectedASMs = graph.argumentStateMaps
    val gotASMs = argumentStates.values.toSeq.sortBy(_.id.x)
    if (expectedASMs != gotASMs) {
      return MatcherWords.equal(expectedASMs).matcher[Seq[ArgumentStateDefinition]].apply(gotASMs)
    }

    val expectedPipelines = pipelines.values.toSeq.sortBy(_.id.x)
    val gotPipelines = graph.pipelines.map { got =>
      MatchablePipeline(got.id,
        (Seq(got.headPlan) ++ got.middlePlans ++ produceResult(got.outputDefinition)).map(_.getClass),
        got.inputBuffer.id,
        out(got.outputDefinition),
        got.serial)

    }
    if (gotPipelines != expectedPipelines) {
      return MatcherWords.equal(expectedPipelines).matcher[Seq[MatchablePipeline]].apply(gotPipelines)
    }

    MatchResult(matches = true, "", "")
  }
}
