/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import java.lang
import java.util
import java.util.concurrent.atomic.AtomicLong

import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.PipelinedRuntimeName
import org.neo4j.cypher.internal.RuntimeName
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.memory.OptionalMemoryTracker
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import scala.collection.JavaConverters.asJavaIterableConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable

/**
 * Can turn an [[ExecutionGraphDefinition]] into a an ExecutionPlan with a hardcoded result of nodes and relationships
 * that visualizes the execution graph. This is intended to be used with the visualization capabilities of Neo4j browser,
 * by preprending a Query with "CYPHER runtime=pipelined debug=visualizePiplines".
 */
object ExecutionGraphVisualizer {

  def getExecutionPlan(executionGraphDefinition: ExecutionGraphDefinition): ExecutionPlan = {
    val rels = mutable.ArrayBuffer[VirtualRelationshipHack]()

    // OPERATORS
    val ops = mutable.Map[Int, VirtualNodeHack]()

    executionGraphDefinition.physicalPlan.logicalPlan.flatten.filter(!_.isInstanceOf[Apply]).foreach { lp =>
      val id = lp.id.x
      val labels = if (lp.isInstanceOf[ProduceResult]) Seq("End", "Operator") else Seq("Operator")
      ops(id) = new VirtualNodeHack(Map("name" -> lp.getClass.getSimpleName, "id" -> (id: Integer)), labels: _*)
    }

    // ARGUMENT STATE MAPS
    val asms = mutable.Map[Int, VirtualNodeHack]()

    executionGraphDefinition.argumentStateMaps.foreach { argumentStateMap =>
      val ArgumentStateDefinition(ArgumentStateMapId(id), Id(planId), argumentSlotOffset) = argumentStateMap
      val asm = new VirtualNodeHack(Map("name" -> s"ASM[$id]", "id" -> (id: Integer), "argumentSlotOffset" -> (argumentSlotOffset: Integer)), "ASM")
      asms(id) = asm
      rels += new VirtualRelationshipHack(ops(planId), asm, Map.empty, "USES_ASM")
    }

    // BUFFERS
    val bufs = mutable.Map[Int, VirtualNodeHack]()

    // Pass 1: create the buffer nodes
    executionGraphDefinition.buffers.foreach { buffer =>
      val BufferDefinition(BufferId(id), _, _, _, variant) = buffer
      variant match {
        case RegularBufferVariant =>
          bufs(id) = new VirtualNodeHack(Map("name" -> s"Buffer[$id]", "id" -> (id: Integer)), "Buffer")
        case ApplyBufferVariant(argumentSlotOffset, _, _, _) =>
          val labels = if (argumentSlotOffset == TopLevelArgument.SLOT_OFFSET) Seq("Start", "Buffer") else Seq("Buffer")
          bufs(id) = new VirtualNodeHack(Map("name" -> s"ApplyBuffer[$id]", "id" -> (id: Integer), "argumentSlotOffset" -> (argumentSlotOffset: Integer)), labels: _*)
        case _: LHSAccumulatingBufferVariant =>
          bufs(id) = new VirtualNodeHack(Map("name" -> s"LHSAccBuffer[$id]", "id" -> (id: Integer)),  "Buffer")
        case _: RHSStreamingBufferVariant =>
          bufs(id) = new VirtualNodeHack(Map("name" -> s"RHSStrBuffer[$id]", "id" -> (id: Integer)),  "Buffer")
        case LHSAccumulatingRHSStreamingBufferVariant(_, _, _, _) =>
          bufs(id) = new VirtualNodeHack(Map("name" -> s"MrBuff[$id]", "id" -> (id: Integer)),  "Buffer")
        case ArgumentStateBufferVariant(_) =>
          bufs(id) = new VirtualNodeHack(Map("name" -> s"ArgumentStateBuffer[$id]", "id" -> (id: Integer)),  "Buffer")
        case ArgumentStreamBufferVariant(_, ArgumentStreamType) =>
          bufs(id) = new VirtualNodeHack(Map("name" -> s"ArgumentStreamBuffer[$id]", "id" -> (id: Integer)),  "Buffer")
        case ArgumentStreamBufferVariant(_, AntiType) =>
          bufs(id) = new VirtualNodeHack(Map("name" -> s"AntiBuffer[$id]", "id" -> (id: Integer)),  "Buffer")
        case AttachBufferVariant(_, _, argumentSlotOffset, _) =>
          bufs(id) = new VirtualNodeHack(Map("name" -> s"AttachBuffer[$id]", "id" -> (id: Integer), "argumentSlotOffset" -> (argumentSlotOffset: Integer)),  "Buffer")
        case ConditionalBufferVariant(_, _, _) =>
          bufs(id) = new VirtualNodeHack(Map("name" -> s"ConditionalSink[$id]", "id" -> (id: Integer)),  "Buffer")
      }
    }
    // Pass 2: connect the buffers
    executionGraphDefinition.buffers.foreach { buffer =>
      val BufferDefinition(BufferId(id), _, reducers, workCancellers, variant) = buffer
      reducers.foreach {
        case ArgumentStateMapId(reducerId) =>
          rels += new VirtualRelationshipHack(bufs(id), asms(reducerId), Map.empty, "HAS_REDUCER")
      }
      workCancellers.foreach {
        case Initialization(ArgumentStateMapId(cancellerId), _) =>
          rels += new VirtualRelationshipHack(bufs(id), asms(cancellerId), Map.empty, "HAS_CANCELLER")
      }
      variant match {
        case ApplyBufferVariant(_, reducersOnRHSReversed, downstreamStates, delegates) =>
          reducersOnRHSReversed.foreach {
            case Initialization(ArgumentStateMapId(reducerId), _) =>
              rels += new VirtualRelationshipHack(bufs(id), asms(reducerId), Map.empty, "HAS_REDUCER_ON_RHS")
          }
          downstreamStates.foreach {
            case ArgumentStateMapId(stateId) =>
              rels += new VirtualRelationshipHack(bufs(id), asms(stateId), Map.empty, "HAS_DOWNSTREAM_STATE")
          }
          delegates.foreach {
            case BufferId(delegateId) =>
              rels += new VirtualRelationshipHack(bufs(id), bufs(delegateId), Map.empty, "DELEGATES_TO")
          }
        case LHSAccumulatingRHSStreamingBufferVariant(lhsSink, rhsSink, _, _) =>
          rels += new VirtualRelationshipHack(bufs(lhsSink.id.x), bufs(id), Map.empty, "DELEGATES_TO")
          rels += new VirtualRelationshipHack(bufs(rhsSink.id.x), bufs(id), Map.empty, "DELEGATES_TO")
        case buf: LHSAccumulatingBufferVariant =>
          rels += new VirtualRelationshipHack(bufs(id), asms(buf.lhsArgumentStateMapId.x), Map.empty, "USES_ASM")
        case buf: RHSStreamingBufferVariant =>
          rels += new VirtualRelationshipHack(bufs(id), asms(buf.rhsArgumentStateMapId.x), Map.empty, "USES_ASM")
        case ArgumentStateBufferVariant(ArgumentStateMapId(asmId)) =>
          rels += new VirtualRelationshipHack(bufs(id), asms(asmId), Map.empty, "USES_ASM")
        case ArgumentStreamBufferVariant(ArgumentStateMapId(asmId), _) =>
          rels += new VirtualRelationshipHack(bufs(id), asms(asmId), Map.empty, "USES_ASM")
        case AttachBufferVariant(applyBuffer, _, _, _) =>
          rels += new VirtualRelationshipHack(bufs(id), bufs(applyBuffer.id.x), Map.empty, "DELEGATES_TO")
        case RegularBufferVariant => // No connections
        case ConditionalBufferVariant(onTrue, onFalse, _) =>
          rels += new VirtualRelationshipHack(bufs(id), bufs(onTrue.id.x), Map.empty, "DELEGATES_TO")
          rels += new VirtualRelationshipHack(bufs(id), bufs(onFalse.id.x), Map.empty, "DELEGATES_TO")
      }
    }

    // PIPELINES
    val pipes = mutable.Map[Int, VirtualNodeHack]()

    executionGraphDefinition.pipelines.foreach { pipeline =>
      val PipelineDefinition(PipelineId(id), lhs, rhs, headPlan, inputBuffer, outputDefinition, middlePlans, serial, _) = pipeline
      val pipe = new VirtualNodeHack(Map("name" -> s"Pipeline[$id]", "id" -> (id: Integer), "serial" -> (serial: lang.Boolean)), "Pipeline")
      pipes(id) = pipe
      rels += new VirtualRelationshipHack(bufs(inputBuffer.id.x), pipe, Map.empty, "READ_BY")
      // Operator chain
      var current = pipe
      headPlan match {
        case FusedHead(fuser) =>
          fuser.fusedPlans.foreach { fusedPlan =>
            val next = ops(fusedPlan.id.x)
            rels += new VirtualRelationshipHack(current, next, Map("fused" -> (true: lang.Boolean)), "NEXT_OPERATOR")
            current = next
          }
        case InterpretedHead(plan) if !plan.isInstanceOf[ProduceResult] =>
          val next = ops(plan.id.x)
          rels += new VirtualRelationshipHack(current, next, Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR")
          current = next

      }

      middlePlans.foreach { middlePlan =>
        val next = ops(middlePlan.id.x)
        rels += new VirtualRelationshipHack(current, next, Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR")
        current = next
      }
      // Output operators, including ProduceResult, are never marked as fused in the ExecutionGraphDefinition,
      // even if they might actually be fused by `FuseOperators`
      outputDefinition match {
        case MorselBufferOutput(BufferId(bufferId), _) =>
          rels += new VirtualRelationshipHack(current, bufs(bufferId), Map.empty, "WRITES_TO")
        case ProduceResultOutput(plan) =>
          rels += new VirtualRelationshipHack(current, ops(plan.id.x), Map("fused" -> (false: lang.Boolean)), "NEXT_OPERATOR")
        case ReduceOutput(BufferId(bufferId), _, _) =>
          rels += new VirtualRelationshipHack(current, bufs(bufferId), Map.empty, "WRITES_TO")
        case MorselArgumentStateBufferOutput(BufferId(bufferId), argumentSlotOffset, _) =>
          rels += new VirtualRelationshipHack(current, bufs(bufferId), Map("argumentSlotOffset" -> (argumentSlotOffset: Integer)), "WRITES_TO")
        case NoOutput => // No connections
      }
    }

    (ops.values ++ asms.values ++ bufs.values ++ pipes.values).map(ValueUtils.fromNodeEntity)

    val nodeList = VirtualValues.list((ops.values ++ asms.values ++ bufs.values ++ pipes.values).map(ValueUtils.fromNodeEntity).toSeq: _*)
    val relList = VirtualValues.list(rels.map(ValueUtils.fromRelationshipEntity): _*)
    new HardcodedResultExecutionPlan(nodeList, relList)
  }

  private class HardcodedResultExecutionPlan(nodes: ListValue, rels: ListValue) extends ExecutionPlan {
    override def run(queryContext: QueryContext,
                     executionMode: ExecutionMode,
                     params: MapValue,
                     prePopulateResults: Boolean,
                     input: InputDataStream,
                     subscriber: QuerySubscriber): RuntimeResult = new HardcodedRuntimeResult(nodes, rels, subscriber)

    override def runtimeName: RuntimeName = PipelinedRuntimeName

    override def metadata: Seq[Argument] = Seq.empty

    override def notifications: Set[InternalNotification] = Set.empty
  }

  private class HardcodedRuntimeResult(nodes: ListValue, rels: ListValue, subscriber: QuerySubscriber) extends RuntimeResult {
    private var demand = false
    private var cancelled = false
    private var returned = false

    subscriber.onResult(2)

    override def fieldNames(): Array[String] = Array("nodes", "relationships")

    override def queryStatistics(): QueryStatistics = new QueryStatistics()

    override def queryProfile(): QueryProfile = QueryProfile.NONE

    override def totalAllocatedMemory(): Long = OptionalMemoryTracker.ALLOCATIONS_NOT_TRACKED

    override def close(): Unit = {}

    override def request(numberOfRecords: Long): Unit = {
      if (numberOfRecords > 0) {
        demand = true
      }
      serveResults()
    }

    override def await(): Boolean = !returned && !cancelled

    override def consumptionState(): RuntimeResult.ConsumptionState =
      if (!returned) {
        ConsumptionState.HAS_MORE
      } else {
        ConsumptionState.EXHAUSTED
      }

    override def cancel(): Unit = cancelled = true

    private def serveResults(): Unit = {
      while (!returned && demand && !cancelled) {
        subscriber.onRecord()
        subscriber.onField(0, nodes)
        subscriber.onField(1, rels)
        subscriber.onRecordCompleted()
        demand = false
        returned = true
      }
      subscriber.onResultCompleted(queryStatistics())
    }
  }

  // Copied hack from SchemaProcedure with small adaptations.

  private object VirtualNodeHack {
    private val MIN_ID = new AtomicLong(-1)
  }

  private class VirtualNodeHack(props: Map[String, AnyRef], labels: String*) extends Node {
    final private val id: Long = VirtualNodeHack.MIN_ID.getAndDecrement

    override def getId: Long = id
    override def getAllProperties: java.util.Map[String, AnyRef] = props.asJava
    override def getLabels: java.lang.Iterable[Label] = labels.map(Label.label).asJava
    override def delete(): Unit = {}
    override def getRelationships: java.lang.Iterable[Relationship] = null
    override def hasRelationship: Boolean = false
    override def getRelationships(types: RelationshipType*): java.lang.Iterable[Relationship] = null
    override def getRelationships(direction: Direction, types: RelationshipType*): java.lang.Iterable[Relationship] = null
    override def getRelationships(direction: Direction): java.lang.Iterable[Relationship] = null
    override def hasRelationship(types: RelationshipType*): Boolean = false
    override def hasRelationship(direction: Direction, types: RelationshipType*): Boolean = false
    override def hasRelationship(direction: Direction): Boolean = false
    override def getSingleRelationship(`type`: RelationshipType, dir: Direction): Relationship = null
    override def createRelationshipTo(otherNode: Node, `type`: RelationshipType): Relationship = null
    override def getRelationshipTypes: java.lang.Iterable[RelationshipType] = null
    override def getDegree: Int = 0
    override def getDegree(`type`: RelationshipType): Int = 0
    override def getDegree(`type`: RelationshipType, direction: Direction): Int = 0
    override def getDegree(direction: Direction): Int = 0
    override def addLabel(label: Label): Unit = {}
    override def removeLabel(label: Label): Unit = {}
    override def hasLabel(label: Label): Boolean = false
    override def hasProperty(key: String): Boolean = false
    override def getProperty(key: String): AnyRef = null
    override def getProperty(key: String, defaultValue: AnyRef): AnyRef = null
    override def setProperty(key: String, value: AnyRef): Unit = {}
    override def removeProperty(key: String): AnyRef = null
    override def getPropertyKeys: java.lang.Iterable[String] = null
    override def getProperties(keys: String*): java.util.Map[String, AnyRef] = null
    override def toString: String = s"VirtualNodeHack[$id]"
  }

  private object VirtualRelationshipHack {
    private val MIN_ID: AtomicLong = new AtomicLong(-1)
  }

  private class VirtualRelationshipHack(startNode: VirtualNodeHack, endNode: VirtualNodeHack, props: Map[String, AnyRef], typ: String) extends Relationship {
    final private val id: Long = VirtualRelationshipHack.MIN_ID.getAndDecrement

    override def getId: Long = id
    override def getStartNode: Node = startNode
    override def getEndNode: Node = endNode
    override def getType: RelationshipType = RelationshipType.withName(typ)
    override def getAllProperties: util.Map[String, AnyRef] = props.asJava
    override def delete(): Unit = {}
    override def getOtherNode(node: Node): Node = null
    override def getNodes: Array[Node] = new Array[Node](0)
    override def isType(typ: RelationshipType): Boolean = false
    override def hasProperty(key: String): Boolean = false
    override def getProperty(key: String): AnyRef = null
    override def getProperty(key: String, defaultValue: AnyRef): AnyRef = null
    override def setProperty(key: String, value: AnyRef): Unit = {}
    override def removeProperty(key: String): AnyRef = null
    override def getPropertyKeys: java.lang.Iterable[String] = null
    override def getProperties(keys: String*): util.Map[String, AnyRef] = null
    override def toString: String = s"VirtualRelationshipHack[$id]"
  }
}
