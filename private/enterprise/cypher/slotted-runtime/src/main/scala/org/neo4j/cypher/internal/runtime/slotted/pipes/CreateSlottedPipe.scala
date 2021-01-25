/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import java.util.function.ToLongFunction

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.LenientCreateRelationship
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.BaseCreatePipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyType
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.exceptions.InvalidSemanticsException
import org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE
import org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP

/**
 * Extends BaseCreatePipe with slotted methods to create nodes and relationships.
 */
abstract class EntityCreateSlottedPipe(source: Pipe) extends BaseCreatePipe(source) {

  /**
   * Create node and return id.
   */
  protected def createNode(context: CypherRow,
                           state: QueryState,
                           command: CreateNodeSlottedCommand): Long = {
    val labelIds = command.labels.map(_.getOrCreateId(state.query)).toArray
    val nodeId = state.query.createNodeId(labelIds)
    command.properties.foreach(setProperties(context, state, nodeId, _, state.query.nodeOps))
    nodeId
  }

  /**
   * Create relationship and return id.
   */
  protected def createRelationship(context: CypherRow,
                                   state: QueryState,
                                   command: CreateRelationshipSlottedCommand): Long = {

    def handleMissingNode(nodeName: String) =
      if (state.lenientCreateRelationship) NO_SUCH_RELATIONSHIP
      else throw new InternalException(LenientCreateRelationship.errorMsg(command.relName, nodeName))

    val startNodeId = command.startNodeIdGetter.applyAsLong(context)
    val endNodeId = command.endNodeIdGetter.applyAsLong(context)
    val typeId = command.relType.getOrCreateType(state.query)

    if (startNodeId == NO_SUCH_NODE) handleMissingNode(command.startName)
    else if (endNodeId == NO_SUCH_NODE) handleMissingNode(command.endName)
    else {
      val relationship = state.query.createRelationship(startNodeId, endNodeId, typeId)
      command.properties.foreach(setProperties(context, state, relationship.id(), _, state.query.relationshipOps))
      relationship.id()
    }

    //    if (startNodeId == -1) {
    //      throw new InternalException(s"Expected to find a node, but found instead: null")
    //    }
    //    if (endNodeId == -1) {
    //      throw new InternalException(s"Expected to find a node, but found instead: null")
    //    }
    //    val relationship = state.query.createRelationship(startNodeId, endNodeId, typeId)
    //    command.properties.foreach(setProperties(context, state, relationship.id(), _, state.query.relationshipOps))
    //    relationship.id
  }
}

case class CreateNodeSlottedCommand(idOffset: Int,
                                    labels: Seq[LazyLabel],
                                    properties: Option[Expression])

case class CreateRelationshipSlottedCommand(relIdOffset: Int,
                                            startNodeIdGetter: ToLongFunction[ReadableRow],
                                            relType: LazyType,
                                            endNodeIdGetter: ToLongFunction[ReadableRow],
                                            properties: Option[Expression],
                                            relName: String,
                                            startName: String,
                                            endName: String)

/**
 * Create nodes and relationships from slotted commands.
 */
case class CreateSlottedPipe(source: Pipe,
                             nodes: IndexedSeq[CreateNodeSlottedCommand],
                             relationships: IndexedSeq[CreateRelationshipSlottedCommand])
                            (val id: Id = Id.INVALID_ID)
  extends EntityCreateSlottedPipe(source) {

  override protected def internalCreateResults(input: ClosingIterator[CypherRow], state: QueryState): ClosingIterator[CypherRow] = {
    input.map {
      row =>
        var i = 0
        while (i < nodes.length) {
          val command = nodes(i)
          val nodeId = createNode(row, state, command)
          row.setLongAt(command.idOffset, nodeId)
          i += 1
        }

        i = 0
        while (i < relationships.length) {
          val command = relationships(i)
          val relationshipId = createRelationship(row, state, command)
          row.setLongAt(command.relIdOffset, relationshipId)
          i += 1
        }

        row
    }
  }

  override protected def handleNoValue(key: String): Unit = {
    // do nothing
  }
}

/**
 * Special create node for use in merge. See `MergeCreateNodePipe`.
 */
case class MergeCreateNodeSlottedPipe(source: Pipe,
                                      command: CreateNodeSlottedCommand)
                                     (val id: Id = Id.INVALID_ID)
  extends EntityCreateSlottedPipe(source) {

  override protected def internalCreateResults(input: ClosingIterator[CypherRow], state: QueryState): ClosingIterator[CypherRow] = {
    input.map {
      row =>
        row.setLongAt(command.idOffset, createNode(row, state, command))
        row
    }
  }

  override protected def handleNoValue(key: String): Unit = {
    val labels = command.labels.map(l => l.name).mkString(":")
    val colon = if (labels.length > 0) {
      ":"
    } else {
      ""
    }
    throw new InvalidSemanticsException(s"Cannot merge the following node because of null property value for '$key': ($colon$labels {$key: null})")
  }
}

/**
 * Special create relationship for use in merge. See `MergeCreateRelationshipPipe`.
 */
case class MergeCreateRelationshipSlottedPipe(source: Pipe,
                                              command: CreateRelationshipSlottedCommand)
                                             (val id: Id = Id.INVALID_ID)
  extends EntityCreateSlottedPipe(source) {

  override protected def internalCreateResults(input: ClosingIterator[CypherRow], state: QueryState): ClosingIterator[CypherRow] = {
    input.map {
      row =>
        row.setLongAt(command.relIdOffset, createRelationship(row, state, command))
        row
    }
  }

  override protected def handleNoValue(key: String): Unit = {
    val startVariableName = command.startName
    val endVariableName = command.endName
    // " UNNAMED X" -> Auto generated variable names, do not expose
    val startVarPart =
      if (startVariableName.startsWith(" ")) {
        ""
      } else {
        startVariableName
      }
    val endVarPart =
      if (endVariableName.startsWith(" ")) {
        ""
      } else {
        endVariableName
      }
    val relType = command.relType.name // this is always available
    throw new InvalidSemanticsException(
      s"Cannot merge the following relationship because of null property value for '$key': ($startVarPart)-[:$relType {$key: null}]->($endVarPart)")
  }
}
