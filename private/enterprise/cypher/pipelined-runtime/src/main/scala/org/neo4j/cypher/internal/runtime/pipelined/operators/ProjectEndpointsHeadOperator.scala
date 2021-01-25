/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.InstanceField
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.arrayLoad
import org.neo4j.codegen.api.IntermediateRepresentation.arrayOf
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.equal
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noValue
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.not
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.ternary
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.codegen.api.Method
import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveRelationshipFromSlotFunctionFor
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.ListSupport
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledHelpers
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.RELATIONSHIP_CURSOR
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.vRELATIONSHIP_CURSOR
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.RelationshipCursorRepresentation
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselWriteCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.ExpandAllOperatorTaskTemplate.loadTypes
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.asListValue
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.cursorNext
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.singleRelationship
import org.neo4j.cypher.internal.runtime.pipelined.operators.ProjectEndpointsMiddleOperatorTemplate.getRelationshipIdFromSlot
import org.neo4j.cypher.internal.runtime.pipelined.operators.ProjectEndpointsMiddleOperatorTemplate.onValidType
import org.neo4j.cypher.internal.runtime.pipelined.operators.VarLengthProjectEndpointsTask.varLengthFindStartAndEnd
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.RelationshipReference
import org.neo4j.values.virtual.RelationshipValue

import scala.collection.mutable.ArrayBuffer

/**
 * Streaming ProjectEndPointsOperator, this is used for the case when ProjectEndpoints
 * is increasing cardinality which is only happening for undirected projections where
 * neither startNode nor endNode is in scope.
 */
class ProjectEndpointsHeadOperator(val workIdentity: WorkIdentity,
                                   relSlot: Slot,
                                   startOffset: Int,
                                   endOffset: Int,
                                   types: RelationshipTypes,
                                   isSimplePath: Boolean) extends StreamingOperator {
  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    if (isSimplePath) {
      singletonIndexedSeq(new ProjectEndpointsTask(workIdentity, inputMorsel.nextCopy, relSlot, startOffset, endOffset, types))
    } else {
      singletonIndexedSeq(new VarLengthProjectEndpointsTask(workIdentity, inputMorsel.nextCopy, relSlot, startOffset, endOffset, types))
    }
  }
}

abstract class BaseProjectEndpointsTask(val workIdentity: WorkIdentity,
                                        inputMorsel: Morsel,
                                        startOffset: Int,
                                        endOffset: Int) extends InputLoopTask(inputMorsel) {

  private var forwardDirection = true
  protected var source: Long = -1L
  protected var target: Long = -1L
  protected var initialized = false

  override def toString: String = "ProjectEndPoints"

  override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
    while (outputRow.onValidRow() && (!forwardDirection || initialized)) {
      initialized = false
      outputRow.copyFrom(inputCursor)
      //For each relationship we write two rows: once in the forward (->) direction and
      //once in the backwards direction (<-)
      if (forwardDirection) {
        outputRow.setLongAt(startOffset, source)
        outputRow.setLongAt(endOffset, target)
        forwardDirection = false
      } else {
        outputRow.setLongAt(startOffset, target)
        outputRow.setLongAt(endOffset, source)

        forwardDirection = true
      }
      outputRow.next()
    }
  }
  override protected def closeInnerLoop(resources: QueryResources): Unit = {}
  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
}

class ProjectEndpointsTask(workIdentity: WorkIdentity,
                           inputMorsel: Morsel,
                           relSlot: Slot,
                           startOffset: Int,
                           endOffset: Int,
                           types: RelationshipTypes)
  extends BaseProjectEndpointsTask(workIdentity, inputMorsel, startOffset, endOffset) {

  private val getRel = makeGetPrimitiveRelationshipFromSlotFunctionFor(relSlot)

  override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
    val relId = getRel.applyAsLong(inputCursor)
    val typesToCheck = types.types(state.query)
    if (entityIsNull(relId)) {
      false
    } else {
      val cursor = resources.expressionCursors.relationshipScanCursor
      state.query.transactionalContext.dataRead.singleRelationship(relId, cursor)
      if (cursor.next() && (typesToCheck == null || typesToCheck.contains(cursor.`type`()))) {
        source = cursor.sourceNodeReference()
        target = cursor.targetNodeReference()
        initialized = true
        true
      } else {
        false
      }
    }
  }
}

class VarLengthProjectEndpointsTask(workIdentity: WorkIdentity,
                                    inputMorsel: Morsel,
                                    relSlot: Slot,
                                    startOffset: Int,
                                    endOffset: Int,
                                    types: RelationshipTypes)
  extends BaseProjectEndpointsTask(workIdentity, inputMorsel, startOffset, endOffset) with ListSupport {

  override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
    val rels = makeTraversable(inputCursor.getRefAt(relSlot.offset))
    val typesToCheck = types.types(state.query)
    varLengthFindStartAndEnd(rels, state, typesToCheck) match {
      case Some((s, e)) =>
        source = s
        target = e
        initialized = true
        true
      case None => false
    }
  }
}

object VarLengthProjectEndpointsTask {
  def varLengthFindStartAndEnd(rels: ListValue,
                               state: PipelinedQueryState,
                               typesToCheck: Array[Int]): Option[(Long, Long)] = {
   val result = varLengthFindStartAndEnd(rels, state.query, typesToCheck)
    if (result == null) None
    else Some((result(0), result(1)))
  }

  /**
   * Used from fused code, returns an array with `Array(start, end)`
   * or `null` if nothing is returned.
   */
  def varLengthFindStartAndEnd(rels: ListValue,
                               qtx: QueryContext,
                               typesToCheck: Array[Int]): Array[Long] = {
    if (rels.isEmpty) {
      return null
    }
    if (typesToCheck == null) {
      val firstRel = rels.head match {
        case relValue: RelationshipValue => relValue
        case relRef: RelationshipReference => qtx.relationshipById(relRef.id())
      }
      val lastRel = rels.last match {
        case relValue: RelationshipValue => relValue
        case relRef: RelationshipReference => qtx.relationshipById(relRef.id())
      }
      Array(firstRel.startNode().id(), lastRel.endNode().id())
    } else {
      var i = 0
      var firstRel: RelationshipValue = null
      var lastRel: RelationshipValue = null
      val numberOfRels = rels.size()
      while (i < numberOfRels) {
        val r = rels.value(i) match {
          case relValue: RelationshipValue => relValue
          case relRef: RelationshipReference => qtx.relationshipById(relRef.id())
        }
        if (!typesToCheck.contains(qtx.relationshipType(r.`type`().stringValue()))) {
          return null
        }
        if (i == 0) {
          firstRel = r
        }
        if (i == numberOfRels - 1) {
          lastRel = r
        }
        i += 1
      }
      Array(firstRel.startNode().id(), lastRel.endNode().id())
    }
  }
}

/**
 * For all cases where ProjectEndPoints doesn't increase cardinality,
 * i.e. all directed projections and undirected projections where at
 * least one node is in scope.
 */
abstract class BaseProjectEndpointsMiddleOperator(val workIdentity: WorkIdentity,
                                                  startOffset: Int,
                                                  startInScope: Boolean,
                                                  endOffset: Int,
                                                  endInScope: Boolean,
                                                  types: RelationshipTypes,
                                                  directed: Boolean) extends StatelessOperator {

  override def operate(morsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {
    val readCursor = morsel.readCursor()
    val writeCursor = morsel.writeCursor(onFirstRow = true)
    val typesToCheck: Array[Int] = types.types(state.query)

    while (readCursor.next()) {
      findStartAndEnd(readCursor, state, resources, typesToCheck) match {
        case Some((start, end)) =>
          writeRow(readCursor, writeCursor, start, end)
        case _ =>
      }
    }
    writeCursor.truncate()
  }

  protected def findStartAndEnd(readCursor: MorselReadCursor,
                              state: PipelinedQueryState,
                              resources: QueryResources,
                              typesToCheck: Array[Int]): Option[(Long, Long)]

  /**
   * When ProjectEndpoints acts as a middle operator we are either a directed projections or if
   * undirected at least one of the two nodes are in scope. That means we will never increase
   * cardinality here and will at most write a single row per relationship.
   */
  private def writeRow(readCursor: MorselReadCursor,
                       writeCursor: MorselWriteCursor,
                       start: Long,
                       end: Long): Unit = {
    if ((!startInScope || readCursor.getLongAt(startOffset) == start) &&
      (!endInScope || readCursor.getLongAt(endOffset) == end)) {
      writeCursor.setLongAt(startOffset, start)
      writeCursor.setLongAt(endOffset, end)
      writeCursor.next()
    } else if (!directed &&
      (!startInScope || readCursor.getLongAt(startOffset) == end) &&
      (!endInScope || readCursor.getLongAt(endOffset) == start)) {
      writeCursor.setLongAt(startOffset, end)
      writeCursor.setLongAt(endOffset, start)
      writeCursor.next()
    } else {
      //skip
    }
  }
}

class ProjectEndpointsMiddleOperator(workIdentity: WorkIdentity,
                                     relSlot: Slot,
                                     startOffset: Int,
                                     startInScope: Boolean,
                                     endOffset: Int,
                                     endInScope: Boolean,
                                     types: RelationshipTypes,
                                     directed: Boolean)
  extends BaseProjectEndpointsMiddleOperator(workIdentity, startOffset, startInScope, endOffset, endInScope, types, directed) {
  private val getRel = makeGetPrimitiveRelationshipFromSlotFunctionFor(relSlot)

  override protected def findStartAndEnd(readCursor: MorselReadCursor,
                                         state: PipelinedQueryState,
                                         resources: QueryResources,
                                         typesToCheck: Array[Int]): Option[(Long, Long)] = {
    val relId = getRel.applyAsLong(readCursor)
    val cursor = resources.expressionCursors.relationshipScanCursor
    if (entityIsNull(relId)) {
      None
    } else {
      state.query.transactionalContext.dataRead.singleRelationship(relId, cursor)
      if (cursor.next() && hasValidType(cursor, typesToCheck)) {
        Some((cursor.sourceNodeReference(), cursor.targetNodeReference()))
      } else {
        None
      }
    }
  }

  private def hasValidType(cursor: RelationshipScanCursor,
                           typesToCheck: Array[Int]): Boolean =
    typesToCheck == null || typesToCheck.contains(cursor.`type`())
}

class VarLengthProjectEndpointsMiddleOperator(workIdentity: WorkIdentity,
                                              relSlot: Slot,
                                              startOffset: Int,
                                              startInScope: Boolean,
                                              endOffset: Int,
                                              endInScope: Boolean,
                                              types: RelationshipTypes,
                                              directed: Boolean)
  extends BaseProjectEndpointsMiddleOperator(workIdentity, startOffset, startInScope, endOffset, endInScope, types, directed) with ListSupport {

  override protected def findStartAndEnd(readCursor: MorselReadCursor,
                                         state: PipelinedQueryState,
                                         resources: QueryResources,
                                         typesToCheck: Array[Int]): Option[(Long, Long)] = {
    varLengthFindStartAndEnd(makeTraversable(readCursor.getRefAt(relSlot.offset)), state, typesToCheck)
  }
}

sealed trait RelationshipTypeLookup
case class ByTokenLookup(token: Int) extends RelationshipTypeLookup
case class ByNameLookup(name: String) extends RelationshipTypeLookup

case class ProjectionTypes(lookup: Seq[RelationshipTypeLookup]) {
  def knownTypes: Seq[Int] = lookup.collect {
    case ByTokenLookup(token) => token
  }
  def missingTypes: Seq[String] = lookup.collect {
    case ByNameLookup(name) => name
  }
}

trait ProjectEndpointsFields {
  protected val knownTypes: Seq[Int] = types.map(_.knownTypes).getOrElse(Seq.empty)

  protected val missingTypes: Seq[String] = types.map(_.missingTypes).getOrElse(Seq.empty)

  protected val typesField: InstanceField = field[Array[Int]](codeGen.namer.nextVariableName("types"),
    if (types.isEmpty) constant(null)
    else arrayOf[Int](knownTypes.map(constant):_*))

  protected val missingTypesField: InstanceField = field[Array[String]](codeGen.namer.nextVariableName("missingTypes"),
    arrayOf[String](missingTypes.map(constant):_*))

  def types: Option[ProjectionTypes]
  def codeGen: OperatorExpressionCompiler
}

/**
 * Code generation template for all cases where ProjectEndPoints doesn't increase cardinality,
 * i.e. all directed projections and undirected projections where at
 * least one node is in scope.
 */
abstract class BaseProjectEndpointsMiddleOperatorTemplate(val inner: OperatorTaskTemplate,
                                                          override val id: Id,
                                                          startOffset: Int,
                                                          startInScope: Boolean,
                                                          endOffset: Int,
                                                          endInScope: Boolean,
                                                          types: Option[ProjectionTypes],
                                                          directed: Boolean,
                                                          codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate with ProjectEndpointsFields  {
  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)

  override def genFields: Seq[Field] = {
    val builder = Seq.newBuilder[Field]
    if (types.nonEmpty) {
      builder += typesField
    }
    if (missingTypes.nonEmpty) {
      builder += missingTypesField
    }
    builder.result()
  }

  /**
   * Depending on what nodes are in scope and whether or not it is directed or not,
   * we must add different conditions before writing to the row.
   */
  protected def writeOps(startVar: String, endVar: String): IntermediateRepresentation = {
    def write(startIR: IntermediateRepresentation, endIR: IntermediateRepresentation) =
      block(
        codeGen.setLongAt(startOffset, startIR),
        codeGen.setLongAt(endOffset, endIR),
        profileRow(id, doProfile),
        inner.genOperateWithExpressions
      )
    if (directed) {
      val gen: IntermediateRepresentation => IntermediateRepresentation =
        if (!startInScope && !endInScope) identity
        else if (!endInScope) {
          //start is in scope, end is not
          condition(equal(codeGen.getLongAt(startOffset), load[Long](startVar)))
        } else if (!startInScope) {
          //end is in scope, start is not
          condition(equal(codeGen.getLongAt(endOffset), load[Long](endVar)))
        } else {
          //both are in scope,
          condition(
            and(
              equal(codeGen.getLongAt(startOffset), load[Long](startVar)),
              equal(codeGen.getLongAt(endOffset), load[Long](endVar))
            )
          )
        }
      gen(write(load[Long](startVar), load[Long](endVar)))
    } else { //undirected
      val newStart = codeGen.namer.nextVariableName()
      val newEnd = codeGen.namer.nextVariableName()
      val init =
        if (!startInScope && !endInScope) throw new IllegalStateException()
        else if (!endInScope) {
          //start is in scope, end is not
          ifElse(equal(codeGen.getLongAt(startOffset), load[Long](startVar))) {
            block(
              assign(newStart, load[Long](startVar)),
              assign(newEnd, load[Long](endVar))
            )
          } { //else
            condition(equal(codeGen.getLongAt(startOffset), load[Long](endVar))) {
              block(
                assign(newStart, load[Long](endVar)),
                assign(newEnd, load[Long](startVar))
              )
            }
          }
        } else if (!startInScope) {
          //end is in scope, start is not
          ifElse(equal(codeGen.getLongAt(endOffset), load[Long](endVar))) {
            block(
              assign(newStart, load[Long](startVar)),
              assign(newEnd, load[Long](endVar))
            )
          } { //else
            condition(equal(codeGen.getLongAt(endOffset), load[Long](startVar))) {
              block(
                assign(newStart, load[Long](endVar)),
                assign(newEnd, load[Long](startVar))
              )
            }
          }
        } else {
          ifElse(
            and(
              equal(codeGen.getLongAt(startOffset), load[Long](startVar)),
              equal(codeGen.getLongAt(endOffset), load[Long](endVar))
            )
          ) {
            block(
              assign(newStart, load[Long](startVar)),
              assign(newEnd, load[Long](endVar))
            )
          } { //else
            condition(
              and(
                equal(codeGen.getLongAt(startOffset), load[Long](endVar)),
                equal(codeGen.getLongAt(endOffset), load[Long](startVar))
              )
            ) {
              block(
                assign(newStart, load[Long](endVar)),
                assign(newEnd, load[Long](startVar))
              )
            }
          }
        }

      block(
        declareAndAssign(typeRefOf[Long], newStart, constant(-1L)),
        declareAndAssign(typeRefOf[Long], newEnd, constant(-1L)),
        init,
        condition(notEqual(load[Long](newStart), constant(-1L)))(write(load[Long](newStart), load[Long](newEnd)))
      )
    }
  }
}

class ProjectEndpointsMiddleOperatorTemplate(inner: OperatorTaskTemplate,
                                             id: Id,
                                             relName: String,
                                             relSlot: Slot,
                                             startOffset: Int,
                                             startInScope: Boolean,
                                             endOffset: Int,
                                             endInScope: Boolean,
                                             val types: Option[ProjectionTypes],
                                             directed: Boolean)
                                            (val codeGen: OperatorExpressionCompiler)
     extends BaseProjectEndpointsMiddleOperatorTemplate(inner, id, startOffset, startInScope, endOffset, endInScope, types, directed, codeGen) {

  override def genOperate: IntermediateRepresentation = {
    val relIdVar = codeGen.namer.nextVariableName()
    val startVar = codeGen.namer.nextVariableName("start")
    val endVar = codeGen.namer.nextVariableName("end")

    //We can have two situations here:
    //  i) We are coming from an expand and have a cursor available, then we just reuse that
    // ii) Otherwise we use the RelationshipScanCursor of ExpressionCursor to read out start and end nodes
    val ops = codeGen.cursorFor(relName) match {
      case Some(cursor: RelationshipCursorRepresentation) =>
        //we have a cursor available for getting the start and end node
        onValidType(cursor.relationshipType, typesField, types.isEmpty) {
          block(
            declareAndAssign(typeRefOf[Long], startVar, cursor.sourceNode),
            declareAndAssign(typeRefOf[Long], endVar, cursor.targetNode),
            writeOps(startVar, endVar)
          )
        }
      case None =>
        //we don't have a cursor available for getting the start and end node
        //do a read.singleRelationship(id, cursor) and read start and end from the cursor
        block(
          declareAndAssign(typeRefOf[Long], relIdVar, getRelationshipIdFromSlot(relSlot, codeGen)),
          condition(notEqual(load[Long](relIdVar), constant(-1L))) {
            block(
              singleRelationship(load[Long](relIdVar), RELATIONSHIP_CURSOR),
              condition(cursorNext[RelationshipScanCursor](RELATIONSHIP_CURSOR)) {
                onValidType(invoke(RELATIONSHIP_CURSOR, method[RelationshipScanCursor, Int]("type")), typesField, types.isEmpty) {
                  block(
                    declareAndAssign(typeRefOf[Long], startVar, invoke(RELATIONSHIP_CURSOR, method[RelationshipScanCursor, Long]("sourceNodeReference"))),
                    declareAndAssign(typeRefOf[Long], endVar, invoke(RELATIONSHIP_CURSOR, method[RelationshipScanCursor, Long]("targetNodeReference"))),
                    writeOps(startVar, endVar)
                  )
                }
              }
            )
          }
        )
    }
    block(
      loadTypes(knownTypes, missingTypes, typesField, missingTypesField),
      ops
    )
  }

  override def genLocalVariables: Seq[LocalVariable] = Seq(vRELATIONSHIP_CURSOR)

  override protected def isHead: Boolean = false
}

object ProjectEndpointsMiddleOperatorTemplate {
  def hasType(typ: Int, types: Array[Int]): Boolean = {
    var i = 0
    while (i < types.length) {
      if (typ == types(i)) return true
      i += 1
    }
    false
  }

  def hasTypeIR: Method = method[ProjectEndpointsMiddleOperatorTemplate, Boolean, Int, Array[Int]]("hasType")

  /**
   * Generate code for type checking if type matching
   */
  def onValidType(typ: IntermediateRepresentation, typeField: InstanceField, skip: Boolean)(ir: IntermediateRepresentation): IntermediateRepresentation = {
    if (skip) ir
    else condition(invokeStatic(hasTypeIR, typ, loadField(typeField)))(ir)
  }

  def getRelationshipIdFromSlot(slot: Slot, codeGen: OperatorExpressionCompiler): IntermediateRepresentation = slot match {
    // NOTE: We do not save the local slot variable, since we are only using it with our own local variable within a local scope
    case LongSlot(offset, _, _) =>
      codeGen.getLongAt(offset)
    case RefSlot(offset, false, _) =>
      invokeStatic(method[CompiledHelpers, Long, AnyValue]("relationshipFromAnyValue"), codeGen.getRefAt(offset))
    case RefSlot(offset, true, _) =>
      ternary(
        equal(codeGen.getRefAt(offset), noValue),
        constant(-1L),
        invokeStatic(method[CompiledHelpers, Long, AnyValue]("relationshipFromAnyValue"), codeGen.getRefAt(offset))
      )
    case _ =>
      throw new InternalException(s"Do not know how to get a node id for slot $slot")
  }
}

class VarLengthProjectEndpointsMiddleOperatorTemplate(inner: OperatorTaskTemplate,
                                                      id: Id,
                                                      relSlot: Slot,
                                                      startOffset: Int,
                                                      startInScope: Boolean,
                                                      endOffset: Int,
                                                      endInScope: Boolean,
                                                      val types: Option[ProjectionTypes],
                                                      directed: Boolean)
                                                     (val codeGen: OperatorExpressionCompiler)
  extends BaseProjectEndpointsMiddleOperatorTemplate(inner, id, startOffset, startInScope, endOffset, endInScope, types, directed, codeGen) {

  override def genOperate: IntermediateRepresentation = {
    val relIdVar = codeGen.namer.nextVariableName()
    val startVar = codeGen.namer.nextVariableName("start")
    val endVar = codeGen.namer.nextVariableName("end")
    val startEnd = codeGen.namer.nextVariableName()
    block(
      loadTypes(knownTypes, missingTypes, typesField, missingTypesField),
      declareAndAssign(typeRefOf[ListValue], relIdVar, asListValue(codeGen.getRefAt(relSlot.offset))),
      declareAndAssign(typeRefOf[Array[Long]], startEnd,
        invokeStatic(
          method[VarLengthProjectEndpointsTask, Array[Long], ListValue, QueryContext, Array[Int]]("varLengthFindStartAndEnd"),
          load[Long](relIdVar), ExpressionCompilation.DB_ACCESS, if (types.isEmpty) constant(null) else loadField(typesField))
        ),
      condition(IntermediateRepresentation.isNotNull(load[Array[Long]](startEnd))) {
        block(
          declareAndAssign(typeRefOf[Long], startVar, arrayLoad(load[Array[Long]](startEnd), 0)),
          declareAndAssign(typeRefOf[Long], endVar, arrayLoad(load[Array[Long]](startEnd), 1)),
          writeOps(startVar, endVar)
        )
      }
    )
  }

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty

  override protected def isHead: Boolean = false
}


abstract class BaseUndirectedProjectEndpointsTaskTemplate(inner: OperatorTaskTemplate,
                                                          id: Id,
                                                          innermost: DelegateOperatorTaskTemplate,
                                                          isHead: Boolean,
                                                          startOffset: Int,
                                                          endOffset: Int,
                                                          val types: Option[ProjectionTypes],
                                                          override val codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen, isHead) with ProjectEndpointsFields {

  protected val forwardField: InstanceField = field[Boolean](codeGen.namer.nextVariableName("forward"), constant(true))
  protected val startField: InstanceField = field[Long](codeGen.namer.nextVariableName("start"), constant(-1L))
  protected val endField: InstanceField = field[Long](codeGen.namer.nextVariableName("end"), constant(-1L))

  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  override final def scopeId: String = "projectEndpoints" + id.x

  override def genMoreFields: Seq[Field] = {
    val builder = ArrayBuffer(forwardField, startField, endField)
    if (knownTypes.nonEmpty) {
      builder += typesField
    }
    if (missingTypes.nonEmpty) {
      builder += missingTypesField
    }
    builder
  }

  override def genLocalVariables: Seq[LocalVariable] = Seq(vRELATIONSHIP_CURSOR)

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  override protected def genInnerLoop: IntermediateRepresentation = {
    block(
      loop(and(innermost.predicate, loadField(canContinue)))(
        block(
          codeGen.copyFromInput(codeGen.inputSlotConfiguration.numberOfLongs,
            codeGen.inputSlotConfiguration.numberOfReferences),
          ifElse(loadField(forwardField)) {
            block(
              codeGen.setLongAt(startOffset, loadField(startField)),
              codeGen.setLongAt(endOffset, loadField(endField)),
            )
          } {//else
            block(
              codeGen.setLongAt(startOffset, loadField(endField)),
              codeGen.setLongAt(endOffset, loadField(startField))
            )
          },
          inner.genOperateWithExpressions,
          doIfInnerCantContinue(
            block(
              profileRow(id, doProfile),
              setField(forwardField, not(loadField(forwardField)))
            )
          ),
          innermost.setUnlessPastLimit(canContinue, not(loadField(forwardField))),
          endInnerLoop
        )
      )
    )
  }

  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    noop()
  }
}

class UndirectedProjectEndpointsTaskTemplate(inner: OperatorTaskTemplate,
                                             id: Id,
                                             innermost: DelegateOperatorTaskTemplate,
                                             isHead: Boolean,
                                             relName: String,
                                             relSlot: Slot,
                                             startOffset: Int,
                                             endOffset: Int,
                                             types: Option[ProjectionTypes])
                                            (codeGen: OperatorExpressionCompiler)
  extends BaseUndirectedProjectEndpointsTaskTemplate(inner, id, innermost, isHead, startOffset, endOffset, types, codeGen) {

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    val ops = codeGen.cursorFor(relName) match {
      case Some(cursor: RelationshipCursorRepresentation) =>
        block(
          setField(canContinue, constant(false)),
          onValidType(cursor.relationshipType, typesField, types.isEmpty) {
            block(
              setField(startField, cursor.sourceNode),
              setField(endField, cursor.targetNode),
              setField(canContinue, constant(true))
            )
          },
          loadField(canContinue))
      case None =>
        val relIdVar = codeGen.namer.nextVariableName()
        block(
          declareAndAssign(typeRefOf[Long], relIdVar, getRelationshipIdFromSlot(relSlot, codeGen)),
          setField(canContinue, constant(false)),
          condition(notEqual(load[Long](relIdVar), constant(-1L))) {
            block(
              singleRelationship(load[Long](relIdVar), RELATIONSHIP_CURSOR),
              condition(cursorNext[RelationshipScanCursor](RELATIONSHIP_CURSOR)) {
                onValidType(invoke(RELATIONSHIP_CURSOR, method[RelationshipScanCursor, Int]("type")), typesField, types.isEmpty) {
                  block(
                    setField(startField, invoke(RELATIONSHIP_CURSOR, method[RelationshipScanCursor, Long]("sourceNodeReference"))),
                    setField(endField, invoke(RELATIONSHIP_CURSOR, method[RelationshipScanCursor, Long]("targetNodeReference"))),
                    setField(canContinue, constant(true))
                  )
                }
              }
            )
          },
          loadField(canContinue)
        )
    }
    block(
      loadTypes(knownTypes, missingTypes, typesField, missingTypesField),
      ops
    )
  }
}

class VarLengthUndirectedProjectEndpointsTaskTemplate(inner: OperatorTaskTemplate,
                                   id: Id,
                                   innermost: DelegateOperatorTaskTemplate,
                                   isHead: Boolean,
                                   relSlot: Slot,
                                   startOffset: Int,
                                   endOffset: Int,
                                   types: Option[ProjectionTypes])
                                  (codeGen: OperatorExpressionCompiler)
  extends BaseUndirectedProjectEndpointsTaskTemplate(inner, id, innermost, isHead, startOffset, endOffset, types, codeGen) {

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    val relIds = codeGen.namer.nextVariableName()
    val startEnd = codeGen.namer.nextVariableName()
    block(
      loadTypes(knownTypes, missingTypes, typesField, missingTypesField),
      declareAndAssign(typeRefOf[ListValue], relIds, asListValue(codeGen.getRefAt(relSlot.offset))),
      setField(canContinue, constant(false)),
      declareAndAssign(typeRefOf[Array[Long]], startEnd,
        invokeStatic(
          method[VarLengthProjectEndpointsTask, Array[Long], ListValue, QueryContext, Array[Int]]("varLengthFindStartAndEnd"),
          load[ListValue](relIds), ExpressionCompilation.DB_ACCESS, if (types.isEmpty) constant(null) else loadField(typesField))
      ),
      condition(IntermediateRepresentation.isNotNull(load[Array[Long]](startEnd))) {
        block(
          setField(startField, arrayLoad(load[Array[Long]](startEnd), 0)),
          setField(endField, arrayLoad(load[Array[Long]](startEnd), 1)),
          setField(canContinue, constant(true))
        )
      },
      loadField(canContinue)
    )
  }
}
