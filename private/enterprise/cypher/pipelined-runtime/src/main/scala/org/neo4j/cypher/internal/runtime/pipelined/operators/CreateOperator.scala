/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.arrayOf
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declare
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.equal
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.isNull
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.ternary
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.runtime.LenientCreateRelationship
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.makeValueNeoSafe
import org.neo4j.cypher.internal.runtime.pipelined.MutableQueryStatistics
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.CreateOperator.createNode
import org.neo4j.cypher.internal.runtime.pipelined.operators.CreateOperator.createRelationship
import org.neo4j.cypher.internal.runtime.pipelined.operators.CreateOperator.handleMissingNode
import org.neo4j.cypher.internal.runtime.pipelined.operators.CreateOperator.setNodeProperties
import org.neo4j.cypher.internal.runtime.pipelined.operators.CreateOperator.setRelationshipProperties
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.DATA_WRITE
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NO_TOKEN
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_STATS_TRACKER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_STATS_TRACKER_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.TOKEN_WRITE
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.conditionallyProfileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.getNodeIdFromSlot
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.pipes.CreateNodeSlottedCommand
import org.neo4j.cypher.internal.runtime.slotted.pipes.CreateRelationshipSlottedCommand
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor
import org.neo4j.internal.kernel.api.TokenWrite
import org.neo4j.internal.kernel.api.Write
import org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE
import org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP
import org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE
import org.neo4j.util.Preconditions
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.MapValue

class CreateOperator(val workIdentity: WorkIdentity,
                     nodes: Array[CreateNodeSlottedCommand],
                     relationships: Array[CreateRelationshipSlottedCommand],
                     lenientCreateRelationship: Boolean) extends StatelessOperator {

  override def operate(morsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {

    val queryState = state.queryStateForExpressionEvaluation(resources)
    val write = state.query.transactionalContext.dataWrite
    val tokenWrite = state.query.transactionalContext.transaction.tokenWrite()

    val cursor: MorselFullCursor = morsel.fullCursor()
    while (cursor.next()) {
      var i = 0
      while (i < nodes.length) {
        val command = nodes(i)
        val labelIds = command.labels.map(_.getOrCreateId(state.query)).toArray
        val nodeId = createNode(labelIds, write, resources.queryStatisticsTracker)
        command.properties.foreach(p => setNodeProperties(nodeId, p(cursor, queryState), tokenWrite, write, resources.queryStatisticsTracker))
        cursor.setLongAt(command.idOffset, nodeId)
        i += 1
      }
      i = 0
      while (i < relationships.length) {
        val command = relationships(i)
        val startNodeId = command.startNodeIdGetter.applyAsLong(cursor)
        val endNodeId = command.endNodeIdGetter.applyAsLong(cursor)
        val typeId = command.relType.getOrCreateType(state.query)
        val relationshipId = if (startNodeId == NO_SUCH_NODE) {
          handleMissingNode(command.relName, command.startName, lenientCreateRelationship)
        } else if (endNodeId == NO_SUCH_NODE) {
          handleMissingNode(command.relName, command.endName, lenientCreateRelationship)
        } else {
          createRelationship(startNodeId, typeId, endNodeId, write, resources.queryStatisticsTracker)
        }
        command.properties.foreach(p => setRelationshipProperties(relationshipId, p(cursor, queryState), tokenWrite, write, resources.queryStatisticsTracker))
        cursor.setLongAt(command.relIdOffset, relationshipId)
        i += 1
      }
    }
  }
}

object CreateOperator {
  def createNode(labels: Array[Int],
                 write: Write,
                 queryStatisticsTracker: MutableQueryStatistics): Long = {
    val nodeId = write.nodeCreateWithLabels(labels)
    queryStatisticsTracker.createNode()
    queryStatisticsTracker.addLabels(labels.length)
    nodeId
  }

  def setNodeProperties(node: Long,
                        properties: AnyValue,
                        tokenWrite: TokenWrite,
                        write: Write,
                        queryStatisticsTracker: MutableQueryStatistics): Unit = {
    safeCastToMap(properties).foreach((k: String, v: AnyValue) => {
      if (!(v eq NO_VALUE)) {
        val propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName(k)
        write.nodeSetProperty(node, propertyKeyId, makeValueNeoSafe(v))
        queryStatisticsTracker.setProperty()
      }
    })
  }

  def handleMissingNode(relName: String, nodeName: String, lenientCreateRelationship: Boolean): Long =
    if (lenientCreateRelationship) NO_SUCH_RELATIONSHIP
    else failOnMissingNode(relName, nodeName)

  def failOnMissingNode(relName: String, nodeName: String): Long =
    throw new InternalException(LenientCreateRelationship.errorMsg(relName, nodeName))

  def createRelationship(source: Long,
                         typ: Int,
                         target: Long,
                         write: Write,
                         queryStatisticsTracker: MutableQueryStatistics): Long = {
      val relId = write.relationshipCreate(source, typ, target)
      queryStatisticsTracker.createRelationship()
      relId
  }

  def setRelationshipProperties(relationship: Long,
                                properties: AnyValue,
                                tokenWrite: TokenWrite,
                                write: Write,
                                queryStatisticsTracker: MutableQueryStatistics): Unit = {
    if (relationship != NO_SUCH_RELATIONSHIP) {
      safeCastToMap(properties).foreach((k: String, v: AnyValue) => {
        if (!(v eq NO_VALUE)) {
          val propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName(k)
          write.relationshipSetProperty(relationship, propertyKeyId, makeValueNeoSafe(v))
          queryStatisticsTracker.setProperty()
        }
      })
    }
  }

  private def safeCastToMap(value: AnyValue): MapValue = value match {
    case mapValue: MapValue => mapValue
    case _ =>
      throw new CypherTypeException(s"Parameter provided for node creation is not a Map, instead got $value")
  }
}
case class CreateNodeFusedCommand(offset: Int, labels: Seq[Either[Int, String]], properties: Option[() => IntermediateExpression])
case class CreateRelationshipFusedCommand(offset: Int,
                                          relName: String,
                                          relType: Either[Int, String],
                                          startName: String,
                                          startSlot: Slot,
                                          endName: String,
                                          endSlot: Slot,
                                          properties: Option[() => IntermediateExpression])

class CreateOperatorTemplate(override val inner: OperatorTaskTemplate,
                             override val id: Id,
                             createNodeCommands: Seq[CreateNodeFusedCommand],
                             createRelationshipCommands: Seq[CreateRelationshipFusedCommand],
                             lenientCreateRelationship: Boolean)(protected val codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {
  private val labelFields = createNodeCommands.map(nc => nc.offset -> field[Array[Int]](codeGen.namer.nextVariableName())).toMap
  private val relTypeFields = createRelationshipCommands.map(rc => rc.offset -> field[Int](codeGen.namer.nextVariableName(), constant(NO_SUCH_RELATIONSHIP_TYPE))).toMap
  private var nodePropertyMap: Map[Int, IntermediateExpression] = _
  private var relPropertyMap: Map[Int, IntermediateExpression] = _

  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  override def genOperate: IntermediateRepresentation = {
    if (nodePropertyMap == null) {
      nodePropertyMap = createNodeCommands.flatMap {
        case CreateNodeFusedCommand(offset, _, Some(p)) => Some(offset -> p())
        case _ => None
      }.toMap
    }
    if (relPropertyMap == null) {
      relPropertyMap = createRelationshipCommands.flatMap {
        case CreateRelationshipFusedCommand(offset, _, _, _, _, _, _, Some(p)) => Some(offset -> p())
        case _ => None
      }.toMap
    }

    val nodeOps = createNodeCommands.map {
      case CreateNodeFusedCommand(offset, labels, properties) =>
        val nodeVar = codeGen.namer.nextVariableName("node")
        block(
          declareAndAssign(typeRefOf[Long], nodeVar,
            invokeStatic(
              method[CreateOperator, Long, Array[Int], Write, MutableQueryStatistics]("createNode"),
              loadField(labelFields(offset)), loadField(DATA_WRITE), QUERY_STATS_TRACKER)
          ),
          properties.map(_ =>
            invokeStatic(method[CreateOperator, Unit, Long, AnyValue, TokenWrite, Write, MutableQueryStatistics]("setNodeProperties"),
              load(nodeVar), nullCheckIfRequired(nodePropertyMap(offset)), loadField(TOKEN_WRITE), loadField(DATA_WRITE), QUERY_STATS_TRACKER)
          ).getOrElse(noop()),
          codeGen.setLongAt(offset, load(nodeVar))
        )
    }

    val relOps = createRelationshipCommands.map {
      case CreateRelationshipFusedCommand(offset, relName, relType, startName, startSlot, endName, endSlot, properties) =>
        val relVar = codeGen.namer.nextVariableName("relationship")
        val startNodeVar = codeGen.namer.nextVariableName("start")
        val endNodeVar = codeGen.namer.nextVariableName("end")

        block(
          declareAndAssign(typeRefOf[Long], startNodeVar, getNodeIdFromSlot(startSlot, codeGen)),
          declareAndAssign(typeRefOf[Long], endNodeVar, getNodeIdFromSlot(endSlot, codeGen)),
          declare[Long](relVar),
          ifElse(or(equal(load(startNodeVar), constant(NO_SUCH_NODE)), equal(load(endNodeVar), constant(NO_SUCH_NODE)))) {
            if (lenientCreateRelationship) {
              assign(relVar, constant(NO_SUCH_RELATIONSHIP))
            } else {
              assign(relVar,
                ternary(equal(load(startNodeVar), constant(NO_SUCH_NODE)),
                  invokeStatic(method[CreateOperator, Long, String, String]("failOnMissingNode"), constant(relName), constant(startName)),
                  invokeStatic(method[CreateOperator, Long, String, String]("failOnMissingNode"), constant(relName), constant(endName))
                )
              )
            }
          } { //else
            assign(relVar,
              invokeStatic(
                method[CreateOperator, Long, Long, Int, Long, Write, MutableQueryStatistics]("createRelationship"),
                load(startNodeVar), loadField(relTypeFields(offset)), load(endNodeVar), loadField(DATA_WRITE), QUERY_STATS_TRACKER)
            )
          },
          properties.map(_ =>
            invokeStatic(method[CreateOperator, Unit, Long, AnyValue, TokenWrite, Write, MutableQueryStatistics]("setRelationshipProperties"),
              load(relVar), nullCheckIfRequired(relPropertyMap(offset)), loadField(TOKEN_WRITE), loadField(DATA_WRITE), QUERY_STATS_TRACKER)
          ).getOrElse(noop()),
          codeGen.setLongAt(offset, load(relVar))
        )
    }
    block(
      block(createNodeCommands.map(setLabelField):_*),
      block(nodeOps:_*),
      block(createRelationshipCommands.map(setRelTypeField):_*),
      block(relOps:_*),
      inner.genOperateWithExpressions,
      conditionallyProfileRow(innerCannotContinue, id, doProfile),
    )
  }

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation =
    inner.genSetExecutionEvent(event)

  override def genExpressions: Seq[IntermediateExpression] = (nodePropertyMap.values ++ relPropertyMap.values).toSeq

  override def genLocalVariables: Seq[LocalVariable] = Seq(QUERY_STATS_TRACKER_V)

  override def genFields: Seq[Field] = Seq(DATA_WRITE, TOKEN_WRITE) ++ labelFields.values ++ relTypeFields.values

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override protected def isHead: Boolean = false

  private def labelGetOrCreate(label: Either[Int, String]) = label match {
    case Left(token) => constant(token)
    case Right(labelName) => invoke(loadField(TOKEN_WRITE), method[TokenWrite, Int, String]("labelGetOrCreateForName"), constant(labelName))
  }

  private def typeGetOrCreate(typ: Either[Int, String]) = typ match {
    case Left(token) => constant(token)
    case Right(typeName) => invoke(loadField(TOKEN_WRITE), method[TokenWrite, Int, String]("relationshipTypeGetOrCreateForName"), constant(typeName))
  }

  private def setLabelField(command: CreateNodeFusedCommand): IntermediateRepresentation = {
    val field = labelFields(command.offset)
    condition(isNull(loadField(field))) {
      setField(field, arrayOf[Int](command.labels.map(labelGetOrCreate):_*))
    }
  }

  private def setRelTypeField(command: CreateRelationshipFusedCommand): IntermediateRepresentation = {
    val field = relTypeFields(command.offset)
    condition(equal(loadField(field), constant(NO_SUCH_RELATIONSHIP_TYPE))) {
      setField(field, typeGetOrCreate(command.relType))
    }
  }
}


