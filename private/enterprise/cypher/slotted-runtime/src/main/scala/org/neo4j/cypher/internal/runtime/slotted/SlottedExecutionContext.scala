/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.physicalplanning.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.{EntityById, ExecutionContext}
import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.util.symbols.{CTNode, CTRelationship}
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.NotFoundException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{Value, Values}
import org.neo4j.values.virtual._

import scala.collection.mutable

object SlottedExecutionContext {
  def empty = new SlottedExecutionContext(SlotConfiguration.empty)
  val DEBUG = false
}

trait SlottedCompatible {
  def copyToSlottedExecutionContext(target: SlottedExecutionContext, nLongs: Int, nRefs: Int): Unit
}

/**
  * Execution context which uses a slot configuration to store values in two arrays.
  *
  * @param slots the slot configuration to use.
  */
//noinspection NameBooleanParameters
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

  override def copyTo(target: ExecutionContext, sourceLongOffset: Int = 0, sourceRefOffset: Int = 0, targetLongOffset: Int = 0, targetRefOffset: Int = 0): Unit =
    target match {
      case other@SlottedExecutionContext(otherPipeline) =>
        if (slots.numberOfLongs > otherPipeline.numberOfLongs ||
          slots.numberOfReferences > otherPipeline.numberOfReferences)
          throw new InternalException(
            s"""A bug has occurred in the slotted runtime: The target slotted execution context cannot hold the data to copy
               |From : $slots
               |To :   $otherPipeline""".stripMargin)
        else {
          System.arraycopy(longs, sourceLongOffset, other.longs, targetLongOffset, slots.numberOfLongs - sourceLongOffset)
          System.arraycopy(refs, sourceRefOffset, other.refs, targetRefOffset, slots.numberOfReferences - sourceRefOffset)
          other.setLinenumber(getLinenumber)
        }
      case _ => fail()
    }

  override def copyFrom(input: ExecutionContext, nLongs: Int, nRefs: Int): Unit =
    if (nLongs > slots.numberOfLongs || nRefs > slots.numberOfReferences)
      throw new InternalException("A bug has occurred in the slotted runtime: The target slotted execution context cannot hold the data to copy.")
    else input match {
      case other@SlottedExecutionContext(_) =>
        System.arraycopy(other.longs, 0, longs, 0, nLongs)
        System.arraycopy(other.refs, 0, refs, 0, nRefs)
        setLinenumber(other.getLinenumber)

      case other: SlottedCompatible =>
        other.copyToSlottedExecutionContext(this, nLongs, nRefs)

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
    if (SlottedExecutionContext.DEBUG && value == null)
      throw new InternalException(s"Reference value not initialised at offset $offset in $this")
    value
  }

  override def getByName(name: String): AnyValue = {
    slots.maybeGetter(name).map(g => g(this)).getOrElse(throw new NotFoundException(s"Unknown variable `$name`."))
  }

  override def numberOfColumns: Int = refs.length + longs.length

  override def containsName(name: String): Boolean = slots.maybeGetter(name).map(g => g(this)).isDefined

  override def setCachedPropertyAt(offset: Int, value: Value): Unit = refs(offset) = value

  override def invalidateCachedNodeProperties(node: Long): Unit = {
    slots.foreachCachedSlot {
      case (cnp, propertyRefSLot) =>
        slots.get(cnp.entityName) match {
          case Some(longSlot: LongSlot) =>
            if (longSlot.typ == CTNode && getLongAt(longSlot.offset) == node) {
              setCachedPropertyAt(propertyRefSLot.offset, null)
            }
          case Some(refSlot: RefSlot) =>
            if (refSlot.typ == CTNode && getRefAt(refSlot.offset).asInstanceOf[VirtualNodeValue].id == node) {
              setCachedPropertyAt(propertyRefSLot.offset, null)
            }
          case None =>
            // This case should not be possible to reach. It is harmless though if it does, which is why no Exception is thrown unless Assertions are enabled
            require(false,
                    s"Tried to invalidate a cached property $cnp but no slot was found for the entity name in $slots.")
        }
    }
  }

  override def invalidateCachedRelationshipProperties(rel: Long): Unit = {
    slots.foreachCachedSlot {
      case (crp, propertyRefSLot) =>
        slots.get(crp.entityName) match {
          case Some(longSlot: LongSlot) =>
            if (longSlot.typ == CTRelationship && getLongAt(longSlot.offset) == rel) {
              setCachedPropertyAt(propertyRefSLot.offset, null)
            }
          case Some(refSlot: RefSlot) =>
            if (refSlot.typ == CTRelationship && getRefAt(refSlot.offset).asInstanceOf[VirtualRelationshipValue].id == rel) {
              setCachedPropertyAt(propertyRefSLot.offset, null)
            }
          case None =>
            // This case should not be possible to reach. It is harmless though if it does, which is why no Exception is thrown unless Assertions are enabled
            require(false,
                    s"Tried to invalidate a cached property $crp but no slot was found for the entity name in $slots.")
        }
    }
  }

  override def setCachedProperty(key: ASTCachedProperty, value: Value): Unit =
    setCachedPropertyAt(slots.getCachedPropertyOffsetFor(key), value)

  override def getCachedPropertyAt(offset: Int): Value = refs(offset).asInstanceOf[Value]

  override def getCachedProperty(key: ASTCachedProperty): Value = fail()

  override def estimatedHeapUsage: Long = {
    var usage = longs.length * 8L
    var i = 0
    while (i < refs.length) {
      val ref = refs(i)
      if (ref != null) {
        usage += ref.estimatedHeapUsage()
      }
      i += 1
    }
    usage
  }

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

  override def mergeWith(other: ExecutionContext, entityById: EntityById): Unit = other match {
    case slottedOther: SlottedExecutionContext =>
      slottedOther.slots.foreachSlot({
        case (key, otherSlot @ LongSlot(offset, _, CTNode)) =>
          val thisSlotSetter = slots.maybePrimitiveNodeSetter(key).getOrElse(
            throw new InternalException(s"Tried to merge primitive node slot $otherSlot from $other but it is missing from $this." +
              "Looks like something needs to be fixed in slot allocation.")
          )
          thisSlotSetter.apply(this, other.getLongAt(offset), entityById)

        case (key, otherSlot @ LongSlot(offset, _, CTRelationship)) =>
          val thisSlotSetter = slots.maybePrimitiveRelationshipSetter(key).getOrElse(
            throw new InternalException(s"Tried to merge primitive relationship slot $otherSlot from $other but it is missing from $this." +
              "Looks like something needs to be fixed in slot allocation.")
          )
          thisSlotSetter.apply(this, other.getLongAt(offset), entityById)

        case (key, otherSlot @ RefSlot(offset, _, _)) if slottedOther.isRefInitialized(offset)  =>
          val thisSlotSetter = slots.maybeSetter(key).getOrElse(
            throw new InternalException(s"Tried to merge slot $otherSlot from $other but it is missing from $this." +
              "Looks like something needs to be fixed in slot allocation.")
          )
          checkOnlyWhenAssertionsAreEnabled(checkCompatibleNullablility(key, otherSlot))

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

  private def checkCompatibleNullablility(key: String, otherSlot: RefSlot): Boolean = {
    val thisSlot = slots.get(key).get
    // This should be guaranteed by slot allocation or else we could get incorrect results
    if (!thisSlot.nullable && otherSlot.nullable)
      throw new InternalException(s"Tried to merge slot $otherSlot into $thisSlot but its nullability is incompatible")
    true
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
    val entities = mutable.Map.empty[String, AnyValue]
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
    }, _ => ()//ignore cached properties
     )
    entities.toMap
  }

  override def isNull(key: String): Boolean =
    slots.get(key) match {
      case Some(RefSlot(offset, true, _)) if isRefInitialized(offset) =>
        getRefAtWithoutCheckingInitialized(offset) eq Values.NO_VALUE
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
