/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declare
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.isNull
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.ast.SlottedCachedProperty
import org.neo4j.cypher.internal.runtime.compiled.expressions.VariableNamer
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_CURSOR

/**
 * This is a specialized OperatorExpressionCompiler used for the case when we have
 * different lhs and rhs slot configurations. Here we must take extra care of how to read
 * cached properties at runtime.
 */
class BinaryOperatorExpressionCompiler(slots: SlotConfiguration,
                                       inputSlotConfiguration: SlotConfiguration,
                                       lhsSlotConfiguration: SlotConfiguration,
                                       rhsSlotConfiguration: SlotConfiguration,
                                       readOnly: Boolean,
                                       namer: VariableNamer) extends OperatorExpressionCompiler(slots, inputSlotConfiguration, readOnly, namer) {
  val fromLHSName: String = namer.nextVariableName("fromLHS")

  /**
   * Here we need to influence how cached properties in the same pipeline are compiled.
   *
   * Since we allocate slots for cached properties that may not exist on both sides and may have different slot offsets on both sides,
   * we have to make decisions if and where to read a cached property from the inputCursor at runtime.
   */
  override def getPropertyCacherAt(property: SlottedCachedProperty, getFromStore: IntermediateRepresentation): PropertyCacher =
    new PropertyCacher(property, getFromStore) {
      // Does the LHS have a slot for this cached property and if so, what is the offset
      private val maybeCachedPropertyOffsetInLHS = lhsSlotConfiguration.getCachedPropertySlot(property).map(_.offset).getOrElse(-1)
      // Does the RHS have a slot for this cached property and if so, what is the offset
      private val maybeCachedPropertyOffsetInRHS = rhsSlotConfiguration.getCachedPropertySlot(property).map(_.offset).getOrElse(-1)
      // The offset (or -1) depending on whether we currently read a morsel from the LHS or from the RHS
      private val maybeOffsetInInputName = namer.nextVariableName("maybeCachedPropertyOffsetInInput")


      // Assign `maybeCachedPropertyOffsetInInput` depending on `fromLHSName`
      override def assignLocalVariables: IntermediateRepresentation = block(
        declare[Int](maybeOffsetInInputName),
        ifElse(load(fromLHSName)) {
          assign(maybeOffsetInInputName, constant(maybeCachedPropertyOffsetInLHS))
        } {
          assign(maybeOffsetInInputName, constant(maybeCachedPropertyOffsetInRHS))
        }
      )

      /**
       * Only try to read from the input cursor of that cursor has a slot for it.
       * Pass in the offset dynamically.
       */
      private def initializeFromContextOrStore: IntermediateRepresentation = {
        block(
          condition(notEqual(load(maybeOffsetInInputName), constant(-1))) {
            assign(local, getCachedPropertyFromExecutionContextWithDynamicOffset(load(maybeOffsetInInputName), INPUT_CURSOR))
          },
          condition(isNull(load(local)))(initializeFromStore)
        )
      }

      override def initializeIfLocalDoesNotExist: IntermediateRepresentation = initializeFromContextOrStore

      override def initializeIfLocalExists: IntermediateRepresentation = initializeFromContextOrStore
    }
}
