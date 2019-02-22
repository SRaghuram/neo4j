/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTNode, CTRelationship, CypherType}
import org.neo4j.cypher.internal.v4_0.util.{AssertionUtils, InternalException, ParameterWrongTypeException}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{VirtualNodeValue, VirtualRelationshipValue, VirtualValues}

object SlotConfigurationUtils {
  // TODO: Check if having try/catch blocks inside some of these generated functions prevents inlining or other JIT optimizations
  //       If so we may want to consider moving the handling and responsibility out to the pipes that use them

  val PRIMITIVE_NULL: Long = -1L

  /**
    * Use this to make a specialized getter function for a slot,
    * that given an ExecutionContext returns an AnyValue.
    */
  def makeGetValueFromSlotFunctionFor(slot: Slot): ExecutionContext => AnyValue =
    slot match {
      case LongSlot(offset, false, CTNode) =>
        (context: ExecutionContext) =>
          VirtualValues.node(context.getLongAt(offset))

      case LongSlot(offset, false, CTRelationship) =>
        (context: ExecutionContext) =>
          VirtualValues.relationship(context.getLongAt(offset))

      case LongSlot(offset, true, CTNode) =>
        (context: ExecutionContext) => {
          val nodeId = context.getLongAt(offset)
          if (nodeId == PRIMITIVE_NULL)
            Values.NO_VALUE
          else
            VirtualValues.node(nodeId)
        }
      case LongSlot(offset, true, CTRelationship) =>
        (context: ExecutionContext) => {
          val relId = context.getLongAt(offset)
          if (relId == PRIMITIVE_NULL)
            Values.NO_VALUE
          else
            VirtualValues.relationship(relId)
        }
      case RefSlot(offset, _, _) =>
        (context: ExecutionContext) =>
          context.getRefAt(offset)

      case _ =>
        throw new InternalException(s"Do not know how to make getter for slot $slot")
    }

  /**
    * Use this to make a specialized getter function for a slot and a primitive return type (i.e. CTNode or CTRelationship),
    * that given an ExecutionContext returns a long.
    */
  def makeGetPrimitiveFromSlotFunctionFor(slot: Slot, returnType: CypherType): ExecutionContext => Long =
    (slot, returnType) match {
      case (LongSlot(offset, _, _), CTNode | CTRelationship) =>
        (context: ExecutionContext) =>
          context.getLongAt(offset)

      case (RefSlot(offset, false, _), CTNode) =>
        (context: ExecutionContext) =>
          val value = context.getRefAt(offset)
          try {
            value.asInstanceOf[VirtualNodeValue].id()
          } catch {
            case _: java.lang.ClassCastException =>
              throw new ParameterWrongTypeException(s"Expected to find a node at ref slot $offset but found $value instead")
          }

      case (RefSlot(offset, false, _), CTRelationship) =>
        (context: ExecutionContext) =>
          val value = context.getRefAt(offset)
          try {
            value.asInstanceOf[VirtualRelationshipValue].id()
          } catch {
            case _: java.lang.ClassCastException =>
              throw new ParameterWrongTypeException(s"Expected to find a relationship at ref slot $offset but found $value instead")
          }

      case (RefSlot(offset, true, _), CTNode) =>
        (context: ExecutionContext) =>
          val value = context.getRefAt(offset)
          try {
            if (value == Values.NO_VALUE)
              PRIMITIVE_NULL
            else
              value.asInstanceOf[VirtualNodeValue].id()
          } catch {
            case _: java.lang.ClassCastException =>
              throw new ParameterWrongTypeException(s"Expected to find a node at ref slot $offset but found $value instead")
          }

      case (RefSlot(offset, true, _), CTRelationship) =>
        (context: ExecutionContext) =>
          val value = context.getRefAt(offset)
          try {
            if (value == Values.NO_VALUE)
              PRIMITIVE_NULL
            else
              value.asInstanceOf[VirtualRelationshipValue].id()
          } catch {
            case _: java.lang.ClassCastException =>
              throw new ParameterWrongTypeException(s"Expected to find a relationship at ref slot $offset but found $value instead")
          }

      case _ =>
        throw new InternalException(s"Do not know how to make a primitive getter for slot $slot with type $returnType")
    }

  /**
    * Use this to make a specialized getter function for a slot that is expected to contain a node
    * that given an ExecutionContext returns a long with the node id.
    */
  def makeGetPrimitiveNodeFromSlotFunctionFor(slot: Slot): ExecutionContext => Long =
    makeGetPrimitiveFromSlotFunctionFor(slot, CTNode)

  /**
    * Use this to make a specialized getter function for a slot that is expected to contain a node
    * that given an ExecutionContext returns a long with the relationship id.
    */
  def makeGetPrimitiveRelationshipFromSlotFunctionFor(slot: Slot): ExecutionContext => Long =
    makeGetPrimitiveFromSlotFunctionFor(slot, CTRelationship)

  /**
    * Use this to make a specialized setter function for a slot,
    * that takes as input an ExecutionContext and an AnyValue.
    */
  def makeSetValueInSlotFunctionFor(slot: Slot): (ExecutionContext, AnyValue) => Unit =
    slot match {
      case LongSlot(offset, false, CTNode) =>
        (context: ExecutionContext, value: AnyValue) =>
          try {
            context.setLongAt(offset, value.asInstanceOf[VirtualNodeValue].id())
          } catch {
            case _: java.lang.ClassCastException =>
              throw new ParameterWrongTypeException(s"Expected to find a node at long slot $offset but found $value instead")
          }

      case LongSlot(offset, false, CTRelationship) =>
        (context: ExecutionContext, value: AnyValue) =>
          try {
            context.setLongAt(offset, value.asInstanceOf[VirtualRelationshipValue].id())
          } catch {
            case _: java.lang.ClassCastException =>
              throw new ParameterWrongTypeException(s"Expected to find a relationship at long slot $offset but found $value instead")
          }

      case LongSlot(offset, true, CTNode) =>
        (context: ExecutionContext, value: AnyValue) =>
          if (value == Values.NO_VALUE)
            context.setLongAt(offset, PRIMITIVE_NULL)
          else {
            try {
              context.setLongAt(offset, value.asInstanceOf[VirtualNodeValue].id())
            } catch {
              case _: java.lang.ClassCastException =>
                throw new ParameterWrongTypeException(s"Expected to find a node at long slot $offset but found $value instead")
            }
          }

      case LongSlot(offset, true, CTRelationship) =>
        (context: ExecutionContext, value: AnyValue) =>
          if (value == Values.NO_VALUE)
            context.setLongAt(offset, PRIMITIVE_NULL)
          else {
            try {
              context.setLongAt(offset, value.asInstanceOf[VirtualRelationshipValue].id())
            } catch {
              case _: java.lang.ClassCastException =>
                throw new ParameterWrongTypeException(s"Expected to find a relationship at long slot $offset but found $value instead")
            }
          }

      case RefSlot(offset, _, _) =>
        (context: ExecutionContext, value: AnyValue) =>
          context.setRefAt(offset, value)

      case _ =>
        throw new InternalException(s"Do not know how to make setter for slot $slot")
    }

  /**
    * Use this to make a specialized setter function for a slot,
    * that takes as input an ExecutionContext and a primitive long value.
    */
  def makeSetPrimitiveInSlotFunctionFor(slot: Slot, valueType: CypherType): (ExecutionContext, Long) => Unit =
    (slot, valueType) match {
      case (LongSlot(offset, nullable, CTNode), CTNode) =>
        if (AssertionUtils.assertionsEnabled && !nullable) {
          (context: ExecutionContext, value: Long) =>
            if (value == PRIMITIVE_NULL)
              throw new ParameterWrongTypeException(s"Cannot assign null to a non-nullable slot")
            context.setLongAt(offset, value)
        }
        else {
          (context: ExecutionContext, value: Long) =>
            context.setLongAt(offset, value)
        }

      case (LongSlot(offset, nullable, CTRelationship), CTRelationship) =>
        if (AssertionUtils.assertionsEnabled && !nullable) {
          (context: ExecutionContext, value: Long) =>
            if (value == PRIMITIVE_NULL)
              throw new ParameterWrongTypeException(s"Cannot assign null to a non-nullable slot")
            context.setLongAt(offset, value)
        }
        else {
          (context: ExecutionContext, value: Long) =>
            context.setLongAt(offset, value)
        }

      case (RefSlot(offset, false, typ), CTNode) if typ.isAssignableFrom(CTNode) =>
        if (AssertionUtils.assertionsEnabled) {
          (context: ExecutionContext, value: Long) =>
            if (value == PRIMITIVE_NULL)
              throw new ParameterWrongTypeException(s"Cannot assign null to a non-nullable slot")
            context.setRefAt(offset, VirtualValues.node(value))
        }
        else {
          (context: ExecutionContext, value: Long) =>
            // NOTE: Slot allocation needs to guarantee that we can never get nulls in here
            context.setRefAt(offset, VirtualValues.node(value))
        }

      case (RefSlot(offset, false, typ), CTRelationship) if typ.isAssignableFrom(CTRelationship) =>
        if (AssertionUtils.assertionsEnabled) {
          (context: ExecutionContext, value: Long) =>
            if (value == PRIMITIVE_NULL)
              throw new ParameterWrongTypeException(s"Cannot assign null to a non-nullable slot")
            context.setRefAt(offset, VirtualValues.relationship(value))
        }
        else {
          (context: ExecutionContext, value: Long) =>
            // NOTE: Slot allocation needs to guarantee that we can never get nulls in here
            context.setRefAt(offset, VirtualValues.relationship(value))
        }

      case (RefSlot(offset, true, typ), CTNode) if typ.isAssignableFrom(CTNode) =>
        (context: ExecutionContext, value: Long) =>
          if (value == PRIMITIVE_NULL)
            context.setRefAt(offset, Values.NO_VALUE)
          else
            context.setRefAt(offset, VirtualValues.node(value))

      case (RefSlot(offset, true, typ), CTRelationship) if typ.isAssignableFrom(CTRelationship) =>
        (context: ExecutionContext, value: Long) =>
          if (value == PRIMITIVE_NULL)
            context.setRefAt(offset, Values.NO_VALUE)
          else
            context.setRefAt(offset, VirtualValues.relationship(value))

      case _ =>
        throw new InternalException(s"Do not know how to make a primitive $valueType setter for slot $slot")
    }

  /**
    * Use this to make a specialized getter function for a slot that is expected to contain a node
    * that given an ExecutionContext returns a long with the node id.
    */
  def makeSetPrimitiveNodeInSlotFunctionFor(slot: Slot): (ExecutionContext, Long) => Unit =
    makeSetPrimitiveInSlotFunctionFor(slot, CTNode)

  /**
    * Use this to make a specialized getter function for a slot that is expected to contain a node
    * that given an ExecutionContext returns a long with the relationship id.
    */
  def makeSetPrimitiveRelationshipInSlotFunctionFor(slot: Slot): (ExecutionContext, Long) => Unit =
    makeSetPrimitiveInSlotFunctionFor(slot, CTRelationship)

  /**
    * Generate and update accessors for all slots in a SlotConfiguration
    */
  def generateSlotAccessorFunctions(slots: SlotConfiguration): Unit = {
    slots.foreachSlot({
      case (key, slot) =>
        val getter = SlotConfigurationUtils.makeGetValueFromSlotFunctionFor(slot)
        val setter = SlotConfigurationUtils.makeSetValueInSlotFunctionFor(slot)
        val primitiveNodeSetter =
          if (slot.typ.isAssignableFrom(CTNode))
            Some(SlotConfigurationUtils.makeSetPrimitiveNodeInSlotFunctionFor(slot))
          else
            None
        val primitiveRelationshipSetter =
          if (slot.typ.isAssignableFrom(CTRelationship))
            Some(SlotConfigurationUtils.makeSetPrimitiveRelationshipInSlotFunctionFor(slot))
          else
            None

        slots.updateAccessorFunctions(key, getter, setter, primitiveNodeSetter, primitiveRelationshipSetter)
    }, notDoingForCachedNodePropertiesYet => {})
  }
}
