/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.physicalplanning.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.v4_0.logical.plans.CachedNodeProperty
import org.neo4j.cypher.internal.v4_0.util.AssertionUtils._
import org.neo4j.cypher.internal.v4_0.util.InternalException
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTNode, CTRelationship}
import org.neo4j.graphdb.NotFoundException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{Value, Values}
import org.neo4j.values.virtual._

import scala.collection.mutable

object SlottedExecutionContext {
  def empty = new SlottedExecutionContext(SlotConfiguration.empty)
}

trait SlottedCompatible {
  def copyToSlottedExecutionContext(target: SlottedExecutionContext, nLongs: Int, nRefs: Int): Unit
}

/**
  * Execution context which uses a slot configuration to store values in two arrays.
  *
  * @param slots the slot configuration to use.
  */
case class SlottedExecutionContext(slots: SlotConfiguration) extends ExecutionContext {

  val longs = new Array[Long](slots.numberOfLongs)
  //java.util.Arrays.fill(longs, -2L) // When debugging long slot issues you can uncomment this to check for uninitialized long slots (also in getLongAt below)
  val refs = new Array[AnyValue](slots.numberOfReferences)

  override def toString: String = {
    val iter = this.iterator
    val s: StringBuilder = StringBuilder.newBuilder
    s ++= s"\nSlottedExecutionContext {\n    $slots"
    while(iter.hasNext) {
      val slotValue = iter.next
      s ++= f"\n    ${slotValue._1}%-40s = ${slotValue._2}"
    }
    s ++= "\n}\n"
    s.result
  }

  override def copyTo(target: ExecutionContext, fromLongOffset: Int = 0, fromRefOffset: Int = 0, toLongOffset: Int = 0, toRefOffset: Int = 0): Unit =
    target match {
      case other@SlottedExecutionContext(otherPipeline) =>
        if (slots.numberOfLongs > otherPipeline.numberOfLongs ||
          slots.numberOfReferences > otherPipeline.numberOfReferences)
          throw new InternalException(
            s"""Tried to copy more data into less:
               |From : ${slots}
               |To :   ${otherPipeline}""".stripMargin)
        else {
          System.arraycopy(longs, fromLongOffset, other.longs, toLongOffset, slots.numberOfLongs - fromLongOffset)
          System.arraycopy(refs, fromRefOffset, other.refs, toRefOffset, slots.numberOfReferences - fromRefOffset)
          other.setLinenumber(getLinenumber)
        }
      case _ => fail()
    }

  override def copyFrom(input: ExecutionContext, nLongs: Int, nRefs: Int): Unit =
    if (nLongs > slots.numberOfLongs || nRefs > slots.numberOfReferences)
      throw new InternalException("Tried to copy more data into less.")
    else input match {
      case other@SlottedExecutionContext(otherPipeline) =>
        System.arraycopy(other.longs, 0, longs, 0, nLongs)
        System.arraycopy(other.refs, 0, refs, 0, nRefs)
        setLinenumber(other.getLinenumber)

      case other: SlottedCompatible =>
        other.copyToSlottedExecutionContext(this, nLongs, nRefs)

      case _ => fail()
    }

  def copyCachedFrom(input: ExecutionContext): Unit = input match {
    case other@SlottedExecutionContext(otherPipeline) =>
      slots.foreachCachedSlot({
        case (key, slot) => setCachedPropertyAt(slot.offset, other.getCachedPropertyAt(otherPipeline.getCachedNodePropertyOffsetFor(key)))
      })
    case _ => fail()
  }

  override def setLongAt(offset: Int, value: Long): Unit =
    longs(offset) = value

  override def getLongAt(offset: Int): Long =
    longs(offset)
    // When debugging long slot issues you can uncomment and replace with this to check for uninitialized long slots
    //  {
    //    val value = longs(offset)
    //    if (value == -2L)
    //      throw new InternalException(s"Long value not initialised at offset $offset in $this")
    //    value
    //  }

  override def setRefAt(offset: Int, value: AnyValue): Unit = refs(offset) = value

  override def getRefAt(offset: Int): AnyValue = {
    val value = refs(offset)
    if (value == null)
      throw new InternalException(s"Reference value not initialised at offset $offset in $this")
    value
  }

  override def getByName(name: String): AnyValue = {
    slots.maybeGetter(name).map(g => g(this)).getOrElse(throw new NotFoundException(s"Unknown variable `$name`."))
  }

  override def numberOfColumns: Int = refs.length + longs.length

  override def containsName(name: String): Boolean = slots.maybeGetter(name).map(g => g(this)).isDefined

  override def setCachedPropertyAt(offset: Int, value: Value): Unit = refs(offset) = value

  override def invalidateCachedProperties(node: Long): Unit = {
    slots.foreachCachedSlot {
      case (cnp, refSlot) =>
        if (getLongAt(slots.getLongOffsetFor(cnp.nodeVariableName)) == node) {
          setCachedPropertyAt(refSlot.offset, null)
        }
    }
  }

  override def setCachedProperty(key: CachedNodeProperty, value: Value): Unit =
    setCachedPropertyAt(slots.getCachedNodePropertyOffsetFor(key), value)

  override def getCachedPropertyAt(offset: Int): Value = refs(offset).asInstanceOf[Value]

  override def getCachedProperty(key: CachedNodeProperty): Value = fail()

  private def fail(): Nothing = throw new InternalException("Tried using a slotted context as a map")

  //-----------------------------------------------------------------------------------------------------------
  // Compatibility implementations of the old ExecutionContext API used by Community interpreted runtime pipes
  //-----------------------------------------------------------------------------------------------------------


  // The newWith methods are called from Community pipes. We should already have allocated slots for the given keys,
  // so we just set the values in the existing slots instead of creating a new context like in the MapExecutionContext.
  override def set(newEntries: Seq[(String, AnyValue)]): Unit =
    newEntries.foreach {
      case (k, v) =>
        setValue(k, v)
    }

  override def set(key1: String, value1: AnyValue): Unit =
    setValue(key1, value1)

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue): Unit = {
    setValue(key1, value1)
    setValue(key2, value2)
  }

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): Unit = {
    setValue(key1, value1)
    setValue(key2, value2)
    setValue(key3, value3)
  }

  override def copyWith(key1: String, value1: AnyValue): ExecutionContext = {
    // This method should throw like its siblings below as soon as reduce is changed to not use it.
    val newCopy = SlottedExecutionContext(slots)
    copyTo(newCopy)
    newCopy.setValue(key1, value1)
    newCopy
  }

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue): ExecutionContext = {
    throw new UnsupportedOperationException(
      "Use ExecutionContextFactory.copyWith instead to get the correct slot configuration"
    )
  }

  override def copyWith(key1: String, value1: AnyValue,
                        key2: String, value2: AnyValue,
                        key3: String, value3: AnyValue): ExecutionContext = {
    throw new UnsupportedOperationException(
      "Use ExecutionContextFactory.copyWith instead to get the correct slot configuration"
    )
  }

  override def copyWith(newEntries: Seq[(String, AnyValue)]): ExecutionContext = {
    throw new UnsupportedOperationException(
      "Use ExecutionContextFactory.copyWith instead to get the correct slot configuration"
    )
  }

  private def setValue(key1: String, value1: AnyValue): Unit = {
    slots.maybeSetter(key1)
      .getOrElse(throw new InternalException(s"Ouch, no suitable slot for key $key1 = $value1\nSlots: $slots"))
      .apply(this, value1)
 }

  def isRefInitialized(offset: Int): Boolean = {
    refs(offset) != null
  }

  def getRefAtWithoutCheckingInitialized(offset: Int): AnyValue =
    refs(offset)

  override def mergeWith(other: ExecutionContext): Unit = other match {
    case slottedOther: SlottedExecutionContext =>
      slottedOther.slots.foreachSlot({
        case (key, otherSlot @ LongSlot(offset, _, CTNode)) =>
          val thisSlotSetter = slots.maybePrimitiveNodeSetter(key).getOrElse(
            throw new InternalException(s"Tried to merge primitive node slot $otherSlot from $other but it is missing from $this." +
              "Looks like something needs to be fixed in slot allocation.")
          )
          thisSlotSetter.apply(this, other.getLongAt(offset))

        case (key, otherSlot @ LongSlot(offset, _, CTRelationship)) =>
          val thisSlotSetter = slots.maybePrimitiveRelationshipSetter(key).getOrElse(
            throw new InternalException(s"Tried to merge primitive relationship slot $otherSlot from $other but it is missing from $this." +
              "Looks like something needs to be fixed in slot allocation.")
          )
          thisSlotSetter.apply(this, other.getLongAt(offset))

        case (key, otherSlot @ RefSlot(offset, _, _)) if slottedOther.isRefInitialized(offset)  =>
          val thisSlotSetter = slots.maybeSetter(key).getOrElse(
            throw new InternalException(s"Tried to merge slot $otherSlot from $other but it is missing from $this." +
              "Looks like something needs to be fixed in slot allocation.")
          )

          ifAssertionsEnabled {
            val thisSlot = slots.get(key).get
            // This should be guaranteed by slot allocation or else we could get incorrect results
            if (!thisSlot.nullable && otherSlot.nullable)
              throw new InternalException(s"Tried to merge slot $otherSlot into $thisSlot but its nullability is incompatible")
          }

          val otherValue = slottedOther.getRefAtWithoutCheckingInitialized(offset)
          thisSlotSetter.apply(this, otherValue)

        case _ =>
        // a slot which is not initialized(=null). This means it is allocated, but will only be used later in the pipeline.
        // Therefore, this is a no-op.
      }, {
        case (cachedNodeProperty, refSlot) =>
          setCachedProperty(cachedNodeProperty, other.getCachedPropertyAt(refSlot.offset))
      })
      setLinenumber(slottedOther.getLinenumber)

    case _ =>
      throw new InternalException("Well well, isn't this a delicate situation?")
  }

  override def createClone(): ExecutionContext = {
    val clone = SlottedExecutionContext(slots)
    copyTo(clone)
    clone
  }

  // TODO: If we save currently utilized slot size per logical plan this could be simplified to checking
  // if the slot offset is less than the current size.
  // This is also the only way that we could detect if a LongSlot was not initialized
  override def boundEntities(materializeNode: Long => AnyValue, materializeRelationship: Long => AnyValue): Map[String, AnyValue] = {
    var entities = mutable.Map.empty[String, AnyValue]
    slots.foreachSlot({
      case (key, RefSlot(offset, _, _)) =>
        if (isRefInitialized(offset)) {
          val entity = getRefAtWithoutCheckingInitialized(offset)
          entity match {
            case _: NodeValue | _: RelationshipValue =>
              entities += key -> entity
            case nodeRef: NodeReference =>
              entities += key -> materializeNode(nodeRef.id())
            case relRef: RelationshipReference =>
              entities += key -> materializeRelationship(relRef.id())
            case _ => // Do nothing
          }
        }
      case (key, LongSlot(offset, false, CTNode)) =>
        entities += key -> materializeNode(getLongAt(offset))
      case (key, LongSlot(offset, false, CTRelationship)) =>
        entities += key -> materializeRelationship(getLongAt(offset))
      case (key, LongSlot(offset, true, CTNode)) =>
        val entityId = getLongAt(offset)
        if (entityId >= 0)
          entities += key -> materializeNode(getLongAt(offset))
      case (key, LongSlot(offset, true, CTRelationship)) =>
        val entityId = getLongAt(offset)
        if (entityId >= 0)
          entities += key -> materializeRelationship(getLongAt(offset))
      case _ => // Do nothing
    }, ignoreCachedNodeProperties => null)
    entities.toMap
  }

  override def isNull(key: String): Boolean =
    slots.get(key) match {
      case Some(RefSlot(offset, true, _)) if isRefInitialized(offset) =>
        getRefAtWithoutCheckingInitialized(offset) == Values.NO_VALUE
      case Some(LongSlot(offset, true, CTNode)) =>
        entityIsNull(getLongAt(offset))
      case Some(LongSlot(offset, true, CTRelationship)) =>
        entityIsNull(getLongAt(offset))
      case _ =>
        false
    }

  private def iterator: Iterator[(String, AnyValue)] = {
    // This method implementation is for debug usage only.
    // Please do not use in production code.
    val longSlots = slots.getLongSlots
    val longSlotValues = for (x <- longSlots)
      yield (x.toString, Values.longValue(longs(x.slot.offset)))

    val refSlots = slots.getRefSlots
    val refSlotValues = for (x <- refSlots)
      yield (x.toString, refs(x.slot.offset))

    val cachedSlots = slots.getCachedPropertySlots
    val cachedPropertySlotValues = for (x <- cachedSlots)
      yield (x.toString, refs(x.slot.offset))

    (longSlotValues ++ refSlotValues ++ cachedPropertySlotValues).iterator
  }
}
