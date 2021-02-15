/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Constructor
import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.newInstance
import org.neo4j.codegen.api.IntermediateRepresentation.noValue
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.IntermediateRepresentation.variable
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.codegen.api.NewInstance
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.MutableQueryStatistics
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.DeleteType.DeleteType
import org.neo4j.cypher.internal.runtime.pipelined.operators.DeleteType.DetachExpression
import org.neo4j.cypher.internal.runtime.pipelined.operators.DeleteType.DetachNode
import org.neo4j.cypher.internal.runtime.pipelined.operators.DeleteType.DetachPath
import org.neo4j.cypher.internal.runtime.pipelined.operators.DeleteType.Expression
import org.neo4j.cypher.internal.runtime.pipelined.operators.DeleteType.Node
import org.neo4j.cypher.internal.runtime.pipelined.operators.DeleteType.Path
import org.neo4j.cypher.internal.runtime.pipelined.operators.DeleteType.Relationship
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_STATE
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_STATS_TRACKER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_STATS_TRACKER_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.conditionallyProfileRow
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InternalException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.NoValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.PathValue
import org.neo4j.values.virtual.RelationshipValue

class DeleteOperator(
  val workIdentity: WorkIdentity,
  deleteExpression: commands.expressions.Expression,
  deleteType: DeleteType
) extends StatelessOperator {
  private[this] var profileEvent: OperatorProfileEvent = _

  override def operate(
    output: Morsel,
    state: PipelinedQueryState,
    resources: QueryResources
  ): Unit = {
    val queryState = state.queryStateForExpressionEvaluation(resources)
    val cursor: MorselFullCursor = output.fullCursor()
    val deleter = Deleter(deleteType, queryState, resources.queryStatisticsTracker, profileEvent)

    while (cursor.next()) {
      val deleteMe = deleteExpression(cursor, queryState)
      if (deleteMe != Values.NO_VALUE) {
        deleter.delete(deleteMe)
      }
    }
  }

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
    this.profileEvent = event
  }
}

class DeleteOperatorTemplate(
  override val inner: OperatorTaskTemplate,
  override val id: Id,
  compileDeleteExpression: () => IntermediateExpression,
  deleteType: DeleteType
)(
  protected val codeGen: OperatorExpressionCompiler
) extends OperatorTaskTemplate {

  private val deleterVar: LocalVariable = variable[Deleter](codeGen.namer.nextVariableName("deleter"), genNewDeleter())
  private val deleteMeVariableName = codeGen.namer.nextVariableName("deleteMe")
  private lazy val deleteExpression: IntermediateExpression = compileDeleteExpression()

  override def genInit: IntermediateRepresentation = inner.genInit

  override def genOperate: IntermediateRepresentation = {
    // Disable cursor re-use over delete. We do this to make sure for example property reads on deleted node throws EntityNotFound.
    codeGen.clearRegisteredCursors()

    block(
      declareAndAssign(typeRefOf[AnyValue], deleteMeVariableName, nullCheckIfRequired(deleteExpression)),
      condition(notEqual(load[AnyValue](deleteMeVariableName), noValue))(
        invoke(load(deleterVar), method[Deleter, Unit, AnyValue]("delete"), load[AnyValue](deleteMeVariableName))
      ),
      inner.genOperateWithExpressions,
      conditionallyProfileRow(innerCannotContinue, id, doProfile)
    )
  }

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = {
    inner.genSetExecutionEvent(event)
  }

  override def genExpressions: Seq[IntermediateExpression] = Seq(deleteExpression)

  override def genLocalVariables: Seq[LocalVariable] = Seq(QUERY_STATS_TRACKER_V, deleterVar)

  override def genFields: Seq[Field] = Seq()

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override protected def isHead: Boolean = false

  private def genNewDeleter(): NewInstance = {
    val deleterType = deleteType match {
      case Node => typeRefOf[NodeDeleter]
      case DetachNode => typeRefOf[DetachNodeDeleter]
      case Relationship => typeRefOf[RelationshipDeleter]
      case Path => typeRefOf[PathDeleter]
      case DetachPath => typeRefOf[DetachPathDeleter]
      case Expression => typeRefOf[AnyDeleter]
      case DetachExpression => typeRefOf[DetachAnyDeleter]
      case unknown => throw new InternalException(s"Unrecognized delete type $unknown")
    }

    newInstance(
      Constructor(deleterType, Seq(typeRefOf[QueryState], typeRefOf[MutableQueryStatistics], typeRefOf[OperatorProfileEvent])),
      QUERY_STATE,
      QUERY_STATS_TRACKER,
      loadField(executionEventField)
    )
  }
}

object DeleteType extends Enumeration {
  type DeleteType = Value
  val Node, DetachNode, Relationship, Path, DetachPath, Expression, DetachExpression = Value
}

trait Deleter {
  /**
   * Deletes the specified value. Note, fails on [[NoValue]]s.
   */
  def delete(deleteMe: AnyValue): Unit
}

object Deleter {
  /**
   * Factory method to create a [[Deleter]] based on the specified delete type.
   */
  def apply(deleteType: DeleteType, state: QueryState, stats: MutableQueryStatistics, profileEvent: OperatorProfileEvent): Deleter = {
    deleteType match {
      case Node => new NodeDeleter(state, stats, profileEvent)
      case DetachNode => new DetachNodeDeleter(state, stats, profileEvent)
      case Relationship => new RelationshipDeleter(state, stats, profileEvent)
      case Path => new PathDeleter(state, stats, profileEvent)
      case DetachPath => new DetachPathDeleter(state, stats, profileEvent)
      case Expression => new AnyDeleter(state, stats, profileEvent)
      case DetachExpression => new DetachAnyDeleter(state, stats, profileEvent)
      case unknown => throw new InternalException(s"Unrecognized delete type $unknown")
    }
  }
}

class NodeDeleter(state: QueryState, stats: MutableQueryStatistics, profileEvent: OperatorProfileEvent) extends BaseDeleter(state, stats, profileEvent) {
  override def delete(deleteMe: AnyValue): Unit = {
    deleteMe match {
      case node: NodeValue => deleteNode(node)
      case _ => throw new CypherTypeException(s"Expected node but got ${deleteMe.getTypeName}")
    }
  }
}

class DetachNodeDeleter(state: QueryState, stats: MutableQueryStatistics, profileEvent: OperatorProfileEvent) extends BaseDeleter(state, stats, profileEvent) {
  override def delete(deleteMe: AnyValue): Unit = {
    deleteMe match {
      case node: NodeValue => detachDeleteNode(node)
      case _ => throw new CypherTypeException(s"Expected node but got ${deleteMe.getTypeName}")
    }
  }
}

class RelationshipDeleter(state: QueryState, stats: MutableQueryStatistics, profileEvent: OperatorProfileEvent) extends BaseDeleter(state, stats, profileEvent) {
  override def delete(deleteMe: AnyValue): Unit = {
    deleteMe match {
      case relationship: RelationshipValue => deleteRelationship(relationship)
      case _ => throw new CypherTypeException(s"Expected relationship but got ${deleteMe.getTypeName}")
    }
  }
}

class PathDeleter(state: QueryState, stats: MutableQueryStatistics, profileEvent: OperatorProfileEvent) extends BaseDeleter(state, stats, profileEvent) {
  override def delete(deleteMe: AnyValue): Unit = {
    deleteMe match {
      case path: PathValue => deletePath(path)
      case _ => throw new CypherTypeException(s"Expected path but got ${deleteMe.getTypeName}")
    }
  }
}

class DetachPathDeleter(state: QueryState, stats: MutableQueryStatistics, profileEvent: OperatorProfileEvent) extends BaseDeleter(state, stats, profileEvent) {
  override def delete(deleteMe: AnyValue): Unit = {
    deleteMe match {
      case path: PathValue => detachDeletePath(path)
      case _ => throw new CypherTypeException(s"Expected path but got ${deleteMe.getTypeName}")
    }
  }
}

class AnyDeleter(state: QueryState, stats: MutableQueryStatistics, profileEvent: OperatorProfileEvent) extends BaseDeleter(state, stats, profileEvent) {
  override def delete(deleteMe: AnyValue): Unit = {
    deleteAny(deleteMe)
  }
}

class DetachAnyDeleter(state: QueryState, stats: MutableQueryStatistics, profileEvent: OperatorProfileEvent) extends BaseDeleter(state, stats, profileEvent) {
  override def delete(deleteMe: AnyValue): Unit = {
    detachDeleteAny(deleteMe)
  }
}

abstract class BaseDeleter(
  private[this] val state: QueryState,
  private[this] val stats: MutableQueryStatistics,
  private[this] val profileEvent: OperatorProfileEvent
) extends Deleter {

  def deleteNode(deleteMe: NodeValue): Unit = {
    // Note, delete operation checks if node is deleted in transaction further down
    if (state.query.nodeOps.delete(deleteMe.id())) {
      stats.deleteNode()
      if (profileEvent != null) {
        profileEvent.dbHit()
      }
    }
  }

  def detachDeleteNode(node: NodeValue): Unit = {
    val id = node.id()
    if (!state.query.nodeOps.isDeletedInThisTx(id)) {
      val deletedRelationships = state.query.detachDeleteNode(id)
      stats.deleteRelationships(deletedRelationships)
      stats.deleteNode()
      if (profileEvent != null) {
        profileEvent.dbHits(1 + deletedRelationships)
      }
    }
  }

  def deleteRelationship(relationship: RelationshipValue): Unit = {
    val id = relationship.id()
    if (state.query.relationshipOps.delete(id)) {
      stats.deleteRelationship()
      if (profileEvent != null) {
        profileEvent.dbHit()
      }
    }
  }

  def deletePath(path: PathValue): Unit = {
    path.nodes().foreach(deleteNode)
    path.relationships().foreach(deleteRelationship)
  }

  def detachDeletePath(path: PathValue): Unit = {
    path.nodes().foreach(detachDeleteNode)
    path.relationships().foreach(deleteRelationship)
  }

  def deleteAny(value: AnyValue): Unit = {
    value match {
      case node: NodeValue => deleteNode(node)
      case relationship: RelationshipValue => deleteRelationship(relationship)
      case path: PathValue => deletePath(path)
      case other =>
        throw new CypherTypeException(s"Expected a Node, Relationship or Path, but got a ${other.getClass.getSimpleName}")
    }
  }

  def detachDeleteAny(value: AnyValue): Unit = {
    value match {
      case node: NodeValue => detachDeleteNode(node)
      case relationship: RelationshipValue => deleteRelationship(relationship)
      case path: PathValue => detachDeletePath(path)
      case other =>
        throw new CypherTypeException(s"Expected a Node, Relationship or Path, but got a ${other.getClass.getSimpleName}")
    }
  }
}
