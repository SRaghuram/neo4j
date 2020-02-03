/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.eclipse.collections.impl.factory.Stacks
import org.eclipse.collections.impl.factory.primitive.LongStacks
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.VariablePredicates
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.RelationshipContainer
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.interpreted.pipes.VarLengthExpandPipe.projectBackwards
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.storageengine.api.RelationshipVisitor
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.RelationshipValue

/**
 * On predicates... to communicate the tested entity to the predicate, expressions
 * variable slots have been allocated. The offsets of these slots are `temp*Offset`.
 * If no predicate exists the offset will be `SlottedPipeMapper.NO_PREDICATE_OFFSET`
 */
case class VarLengthExpandSlottedPipe(source: Pipe,
                                      fromSlot: Slot,
                                      relOffset: Int,
                                      toSlot: Slot,
                                      dir: SemanticDirection,
                                      projectedDir: SemanticDirection,
                                      types: RelationshipTypes,
                                      min: Int,
                                      maxDepth: Option[Int],
                                      shouldExpandAll: Boolean,
                                      slots: SlotConfiguration,
                                      tempNodeOffset: Int,
                                      tempRelationshipOffset: Int,
                                      nodePredicate: Expression,
                                      relationshipPredicate: Expression,
                                      argumentSize: SlotConfiguration.Size)
                                     (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {
  type LNode = Long

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot, throwOnTypeError = false)
  private val getToNodeFunction =
    if (shouldExpandAll) {
      null
    } // We only need this getter in the ExpandInto case
    else {
      makeGetPrimitiveNodeFromSlotFunctionFor(toSlot, throwOnTypeError = false)
    }
  private val toOffset = toSlot.offset

  nodePredicate.registerOwningPipe(this)
  relationshipPredicate.registerOwningPipe(this)

  //===========================================================================
  // Runtime code
  //===========================================================================


  private def varLengthExpand(node: LNode,
                              state: QueryState,
                              row: ExecutionContext): Iterator[(LNode, RelationshipContainer)] = {
    val stackOfNodes = LongStacks.mutable.empty()
    val stackOfRelContainers = Stacks.mutable.empty[RelationshipContainer]()
    stackOfNodes.push(node)
    stackOfRelContainers.push(RelationshipContainer.EMPTY)

    new Iterator[(LNode, RelationshipContainer)] {
      override def next(): (LNode, RelationshipContainer) = {
        val fromNode = stackOfNodes.pop()
        val rels = stackOfRelContainers.pop()
        if (rels.size < maxDepth.getOrElse(Int.MaxValue)) {
          val relationships: RelationshipIterator = state.query.getRelationshipsForIdsPrimitive(fromNode, dir, types.types(state.query))

          var relationship: RelationshipValue = null

          val relVisitor = new RelationshipVisitor[InternalException] {
            override def visit(relationshipId: Long, typeId: Int, startNodeId: LNode, endNodeId: LNode): Unit = {

              relationship = state.query.relationshipById(relationshipId, startNodeId, endNodeId, typeId)
            }
          }

          while (relationships.hasNext) {
            val relId = relationships.next()
            relationships.relationshipVisit(relId, relVisitor)
            val relationshipIsUniqueInPath = !rels.contains(relationship)

            if (relationshipIsUniqueInPath) {
              // Before expanding, check that both the relationship and node in question fulfil the predicate
              if (predicateIsTrue(row, state, tempRelationshipOffset, relationshipPredicate, state.query.relationshipById(relId)) &&
                predicateIsTrue(row, state, tempNodeOffset, nodePredicate, state.query.nodeById(relationship.otherNodeId(fromNode)))
              ) {
                // TODO: This call creates an intermediate NodeEntity which should not be necessary
                stackOfNodes.push(relationship.otherNodeId(fromNode))
                stackOfRelContainers.push(rels.append(relationship))
              }
            }
          }
        }
        val projectedRels =
          if (projectBackwards(dir, projectedDir)) {
            rels.reverse
          } else {
            rels
          }

        (fromNode, projectedRels)
      }

      override def hasNext: Boolean = !stackOfNodes.isEmpty
    }
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap {
      inputRow =>
        val fromNode = getFromNodeFunction.applyAsLong(inputRow)
        if (entityIsNull(fromNode)) {
          Iterator.empty
        }
        else {
          // Ensure that the start-node also adheres to the node predicate
          if (predicateIsTrue(inputRow, state, tempNodeOffset, nodePredicate, state.query.nodeById(fromNode))) {

            val paths: Iterator[(LNode, RelationshipContainer)] = varLengthExpand(fromNode, state, inputRow)
            paths collect {
              case (toNode: LNode, rels: RelationshipContainer)
                if rels.size >= min && isToNodeValid(inputRow, toNode) =>
                val resultRow = SlottedExecutionContext(slots)
                resultRow.copyFrom(inputRow, argumentSize.nLongs, argumentSize.nReferences)
                if (shouldExpandAll) {
                  resultRow.setLongAt(toOffset, toNode)
                }
                resultRow.setRefAt(relOffset, rels.asList)
                resultRow
            }
          }
          else {
            Iterator.empty
          }
        }
    }
  }

  private def predicateIsTrue(row: ExecutionContext,
                              state: QueryState,
                              tempOffset: Int,
                              predicate: Expression,
                              entity: AnyValue): Boolean =
    tempOffset == VariablePredicates.NO_PREDICATE_OFFSET || {
      state.expressionVariables(tempOffset) = entity
      predicate(row, state) eq Values.TRUE
    }

  private def isToNodeValid(row: ExecutionContext, node: LNode): Boolean =
    shouldExpandAll || getToNodeFunction.applyAsLong(row) == node
}
