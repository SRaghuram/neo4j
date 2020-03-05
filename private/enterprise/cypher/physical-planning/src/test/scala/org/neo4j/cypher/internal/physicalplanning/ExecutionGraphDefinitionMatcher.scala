/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.physicalplanning
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition.NO_ARGUMENT_STATE_MAPS
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition.NO_ARGUMENT_STATE_MAP_INITIALIZATIONS
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition.NO_BUFFERS
import org.neo4j.cypher.internal.util.attribution.Id
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher
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

  private case object ProduceResultOutput extends MatchableOutputDefinition

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
      case physicalplanning.MorselBufferOutput(id, _) => MorselBufferOutput(id)
      case physicalplanning.MorselArgumentStateBufferOutput(id, argumentSlotOffset, _) => MorselArgumentStateBufferOutput(id, argumentSlotOffset)
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

    private val UNKNOWN_ARG_SLOT_OFFSET = -2

    def applyBuffer(id: Int, argumentSlotOffset: Int = UNKNOWN_ARG_SLOT_OFFSET, planId: Int = -1): ApplyBufferSequence = {
      val bd = buffers.getOrElseUpdate(id, BufferDefinition(BufferId(id),
        Id(planId),
        NO_ARGUMENT_STATE_MAPS,
        NO_ARGUMENT_STATE_MAPS,
        NO_ARGUMENT_STATE_MAPS,
        ApplyBufferVariant(
          if (argumentSlotOffset == UNKNOWN_ARG_SLOT_OFFSET)
            throw new IllegalArgumentException("You have to specify the argumentSlotOffset for new apply buffers")
          else argumentSlotOffset,
          NO_ARGUMENT_STATE_MAP_INITIALIZATIONS,
          NO_BUFFERS))(SlotConfiguration.empty))
      new ApplyBufferSequence(bd)
    }

    def morselBuffer(id: Int): MorselBufferSequence = {
      // This buffer must have been specified before
      new MorselBufferSequence(buffers(id))
    }
  }

  abstract class BufferSequence(bufferDefinition: BufferDefinition) {
    def reducer(id: Int, planId: Int = -1, argumentSlotOffset: Int = -1): ExecutionGraphDefinitionMatcher = {
      val asd = registerArgumentState(id, planId, argumentSlotOffset)
      buffers(bufferDefinition.id.x) = bufferDefinition.withReducers(bufferDefinition.reducers :+ asd.id)
      ExecutionGraphDefinitionMatcher.this
    }

    def canceller(id: Int, planId: Int = -1, argumentSlotOffset: Int = -1): ExecutionGraphDefinitionMatcher = {
      val asd = registerArgumentState(id, planId, argumentSlotOffset)
      buffers(bufferDefinition.id.x) = bufferDefinition.withWorkCancellers(bufferDefinition.workCancellers :+ asd.id)
      ExecutionGraphDefinitionMatcher.this
    }
  }

  class ApplyBufferSequence(bufferDefinition: BufferDefinition) extends BufferSequence(bufferDefinition) {

    private val variant = bufferDefinition.variant.asInstanceOf[ApplyBufferVariant]

    def delegateToMorselBuffer(id: Int, planId: Int = -1): MorselBufferSequence = {
      val bd = buffers.getOrElseUpdate(id,
        BufferDefinition(
          BufferId(id),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          RegularBufferVariant)(SlotConfiguration.empty))
      val updatedApplyBuffer = bufferDefinition.copy(variant = variant.copy(delegates = variant.delegates :+ bd.id))(SlotConfiguration.empty)
      buffers(bufferDefinition.id.x) = updatedApplyBuffer

      updateAttachBuffers(updatedApplyBuffer)

      new MorselBufferSequence(bd)
    }

    def reducerOnRHS(id: Int, planId: Int = -1, argumentSlotOffset: Int = -1, initialCount: Int = 1): ExecutionGraphDefinitionMatcher = {
      val asd = registerArgumentState(id, planId, argumentSlotOffset)
      val updatedApplyBuffer = bufferDefinition.copy(variant =
        variant.copy(reducersOnRHSReversed = Initialization(asd.id, initialCount) +: variant.reducersOnRHSReversed)
      )(SlotConfiguration.empty)
      buffers(bufferDefinition.id.x) = updatedApplyBuffer

      updateAttachBuffers(updatedApplyBuffer)

      ExecutionGraphDefinitionMatcher.this
    }

    private def updateAttachBuffers(updatedApplyBuffer: BufferDefinition) = {
      buffers.transform {
        case (_, bd@BufferDefinition(_, _, _, _, _, atv@AttachBufferVariant(`bufferDefinition`, _, _, _))) =>
          bd.copy(variant = atv.copy(applyBuffer = updatedApplyBuffer))(SlotConfiguration.empty)
        case (_, x) => x
      }
    }
  }

  class AttachBufferSequence(bufferDefinition: BufferDefinition) extends BufferSequence(bufferDefinition) {

    private val variant = bufferDefinition.variant.asInstanceOf[AttachBufferVariant]

    def lhsJoinSinkForAttach(lhsSinkId: Int, lhsAsmId: Int, planId: Int): AttachBufferSequence = {
      /*
      Create LHS sink of join buffer, but do not set as output.
      Attach buffer only needs the ASM of LHS Sink.
       */
      buffers.getOrElseUpdate(lhsSinkId,
        BufferDefinition(
          BufferId(lhsSinkId),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          variant = LHSAccumulatingBufferVariant(ArgumentStateMapId(lhsAsmId)),
        )(SlotConfiguration.empty))
      this
    }

    def delegateToApplyBuffer(id: Int, argumentSlotOffset: Int = -1, planId: Int = -1): ApplyBufferSequence = {
      val bd = buffers.getOrElseUpdate(id,
        BufferDefinition(
          BufferId(id),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          ApplyBufferVariant(argumentSlotOffset, NO_ARGUMENT_STATE_MAP_INITIALIZATIONS, NO_BUFFERS))(SlotConfiguration.empty))

      buffers(bufferDefinition.id.x) = bufferDefinition.copy(variant = variant.copy(applyBuffer = bd))(SlotConfiguration.empty)

      new ApplyBufferSequence(bd)
    }
  }

  abstract class BufferBeforePipelineSequence(bufferDefinition: BufferDefinition) extends BufferSequence(bufferDefinition) {
    def pipeline(id: Int, plans: Seq[Class[_ <: LogicalPlan]] = Seq.empty, serial: Boolean = false): PipelineSequence = {
      val mp = pipelines.getOrElseUpdate(id, MatchablePipeline(PipelineId(id), plans, bufferDefinition.id, null, serial))
      new PipelineSequence(mp)
    }

  }

  class MorselBufferSequence(bufferDefinition: BufferDefinition) extends BufferBeforePipelineSequence(bufferDefinition)

  class ArgumentStateBufferSequence(bufferDefinition: BufferDefinition) extends BufferBeforePipelineSequence(bufferDefinition)

  class OptionalBufferSequence(bufferDefinition: BufferDefinition) extends BufferBeforePipelineSequence(bufferDefinition)

  class AntiBufferSequence(bufferDefinition: BufferDefinition) extends BufferBeforePipelineSequence(bufferDefinition)

  class LhsJoinBufferSequence(bufferDefinition: BufferDefinition) extends BufferBeforePipelineSequence(bufferDefinition)

  class JoinBufferSequence(bufferDefinition: BufferDefinition) extends BufferBeforePipelineSequence(bufferDefinition)

  class PipelineSequence(matchablePipeline: MatchablePipeline) {
    def morselBuffer(id: Int, planId: Int = -1) : MorselBufferSequence = {
      val bd = buffers.getOrElseUpdate(id,
        BufferDefinition(
          BufferId(id),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          RegularBufferVariant)(SlotConfiguration.empty))
      val out = MorselBufferOutput(BufferId(id))
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      new MorselBufferSequence(bd)
    }

    def argumentStateBuffer(id: Int, asmId: Int = -1, planId: Int = -1): ArgumentStateBufferSequence = {
      val bd = buffers.getOrElseUpdate(id,
        BufferDefinition(
          BufferId(id),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          ArgumentStateBufferVariant(ArgumentStateMapId(asmId)))(SlotConfiguration.empty))
      val out = ReduceOutput(BufferId(id))
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      new ArgumentStateBufferSequence(bd)
    }

    def optionalBuffer(id: Int, argumentSlotOffset: Int,  asmId: Int = -1, planId: Int = -1): OptionalBufferSequence = {
      val bd = buffers.getOrElseUpdate(id,
        BufferDefinition(
          BufferId(id),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          OptionalBufferVariant(ArgumentStateMapId(asmId), OptionalType))(SlotConfiguration.empty))
      val out = MorselArgumentStateBufferOutput(BufferId(id),argumentSlotOffset)
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      new OptionalBufferSequence(bd)
    }

    def antiBuffer(id: Int, argumentSlotOffset: Int,  asmId: Int = -1, planId: Int = -1): AntiBufferSequence = {
      val bd = buffers.getOrElseUpdate(id,
        BufferDefinition(
          BufferId(id),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          OptionalBufferVariant(ArgumentStateMapId(asmId), AntiType))(SlotConfiguration.empty))
      val out = MorselArgumentStateBufferOutput(BufferId(id),argumentSlotOffset)
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      new AntiBufferSequence(bd)
    }

    def attachBuffer(id: Int, argumentSlotOffset: Int = -1, planId: Int = -1, slots: SlotConfiguration = SlotConfiguration.empty) : AttachBufferSequence = {
      val bd = buffers.getOrElseUpdate(id,
        BufferDefinition(
          BufferId(id),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          AttachBufferVariant(null, slots, argumentSlotOffset, SlotConfiguration.Size.zero)
        )(SlotConfiguration.empty)
      )
      val out = MorselBufferOutput(BufferId(id))
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      new AttachBufferSequence(bd)
    }

    def applyBuffer(id: Int, argumentSlotOffset: Int = -1, planId: Int = -1): ApplyBufferSequence = {
      val bd = buffers.getOrElseUpdate(id,
        BufferDefinition(
          BufferId(id),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          ApplyBufferVariant(argumentSlotOffset, NO_ARGUMENT_STATE_MAP_INITIALIZATIONS, NO_BUFFERS))(SlotConfiguration.empty))
      val out = MorselBufferOutput(BufferId(id))
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      new ApplyBufferSequence(bd)
    }

    def leftOfJoinBuffer(id: Int, argumentSlotOffset: Int, asmId: Int, planId: Int): LhsJoinBufferSequence = {
      val out = MorselArgumentStateBufferOutput(BufferId(id), argumentSlotOffset)
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      val lhsSink: BufferDefinition = buffers.getOrElseUpdate(id,
        BufferDefinition(
          BufferId(id),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          variant = LHSAccumulatingBufferVariant(ArgumentStateMapId(asmId)),
        )(SlotConfiguration.empty))
      new LhsJoinBufferSequence(lhsSink)
    }

    def rightOfJoinBuffer(lhsId: Int, rhsId: Int, sourceId: Int, argumentSlotOffset: Int, rhsAsmId: Int, planId: Int): JoinBufferSequence = {
      val out = MorselArgumentStateBufferOutput(BufferId(rhsId), argumentSlotOffset)
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      val lhsSink: BufferDefinition = buffers.getOrElse(lhsId, throw new IllegalStateException(s"LHS Sink '$lhsId' should have already been created"))
      val rhsSink: BufferDefinition = buffers.getOrElseUpdate(rhsId,
        BufferDefinition(
          BufferId(rhsId),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAPS,
          variant = RHSStreamingBufferVariant(ArgumentStateMapId(rhsAsmId)),
        )(SlotConfiguration.empty))
      val bd: BufferDefinition = buffers.getOrElseUpdate(sourceId,
                                       BufferDefinition(
                                         BufferId(sourceId),
                                         Id(planId),
                                         NO_ARGUMENT_STATE_MAPS,
                                         NO_ARGUMENT_STATE_MAPS,
                                         NO_ARGUMENT_STATE_MAPS,
                                         LHSAccumulatingRHSStreamingBufferVariant(
                                           lhsSink,
                                           rhsSink,
            lhsSink.variant.asInstanceOf[LHSAccumulatingBufferVariant].argumentStateMapId,
            rhsSink.variant.asInstanceOf[RHSStreamingBufferVariant].argumentStateMapId)
                                       )(SlotConfiguration.empty))
      new JoinBufferSequence(bd)
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

    val expectedASMs = argumentStates.values.toSeq.sortBy(_.id.x)
    val gotASMs = graph.argumentStateMaps
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
