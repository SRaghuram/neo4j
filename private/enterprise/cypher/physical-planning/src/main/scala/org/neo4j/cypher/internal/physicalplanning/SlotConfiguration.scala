/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.ApplyPlanSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.CachedPropertySlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlotKey
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.EntityById
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.exceptions.InternalException
import org.neo4j.values.AnyValue

import scala.collection.mutable

object SlotConfiguration {
  def empty = new SlotConfiguration(mutable.Map.empty, 0, 0)

  // This method uses `empty`, `newLong`, and `newReference`, instead of passing in a Map, so that aliases are computed correctly.
  def apply(slots: Map[String, Slot], numberOfLongs: Int, numberOfReferences: Int): SlotConfiguration = {
    val stringToSlot = mutable.Map[SlotKey, Slot](slots.toSeq.map(kv => (VariableSlotKey(kv._1), kv._2)): _*)
    new SlotConfiguration(stringToSlot, numberOfLongs, numberOfReferences)
  }

  case class Size(nLongs: Int, nReferences: Int)
  object Size {
    val zero = Size(nLongs = 0, nReferences = 0)
  }

  final def isRefSlotAndNotAlias(slots: SlotConfiguration, k: String): Boolean = {
    !slots.isAlias(k) &&
      slots.get(k).forall(_.isInstanceOf[RefSlot])
  }

  sealed trait SlotKey
  case class VariableSlotKey(name: String) extends SlotKey
  case class CachedPropertySlotKey(property: ASTCachedProperty) extends SlotKey
  case class ApplyPlanSlotKey(applyPlanId: Id) extends SlotKey
}

/**
 * A configuration which maps variables to slots. Two types of slot exists: LongSlot and RefSlot. In LongSlots we
 * store nodes and relationships, represented by their ids, and in RefSlots everything else, represented as AnyValues.
 *
 * @param slots the slots of the configuration.
 * @param numberOfLongs the number of long slots.
 * @param numberOfReferences the number of ref slots.
 */
class SlotConfiguration private(private val slots: mutable.Map[SlotConfiguration.SlotKey, Slot],
                        var numberOfLongs: Int,
                        var numberOfReferences: Int) {


  // For each existing variable key, a mapping to all aliases.
  // If x is added first, and y and z are aliases of x, the mapping will look like "x" -> Set("y", "z")
  // Contains only information about VariableSlotKeys
  private val slotAliases = new mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]

  private val getters: mutable.Map[String, CypherRow => AnyValue] = new mutable.HashMap[String, CypherRow => AnyValue]()
  private val setters: mutable.Map[String, (CypherRow, AnyValue) => Unit] = new mutable.HashMap[String, (CypherRow, AnyValue) => Unit]()
  private val primitiveNodeSetters: mutable.Map[String, (CypherRow, Long, EntityById) => Unit] = new mutable.HashMap[String, (CypherRow, Long, EntityById) => Unit]()
  private val primitiveRelationshipSetters: mutable.Map[String, (CypherRow, Long, EntityById) => Unit] = new mutable.HashMap[String, (CypherRow, Long, EntityById) => Unit]()

  def size() = SlotConfiguration.Size(numberOfLongs, numberOfReferences)

  def addAlias(newKey: String, existingKey: String): SlotConfiguration = {
    val slot = slots.getOrElse(VariableSlotKey(existingKey),
      throw new SlotAllocationFailed(s"Tried to alias non-existing slot '$existingKey'  with alias '$newKey'"))
    slots.put(VariableSlotKey(newKey), slot)
    slotAliases.addBinding(existingKey, newKey)
    this
  }

  /**
   * Test if a slot key refers to an alias.
   * NOTE: method can only test keys that are either 'original key' or alias, MUST NOT be called on keys that are neither (i.e., do not exist in the configuration).
   */
  def isAlias(key: String): Boolean = {
    !slotAliases.contains(key)
  }

  def apply(key: String): Slot = slots.apply(VariableSlotKey(key))

  def nameOfLongSlot(offset: Int): Option[String] = slots.collectFirst {
    case (VariableSlotKey(name), LongSlot(o, _, _)) if o == offset && !isAlias(name) => name
  }

  def filterSlots[U](f: ((SlotKey,Slot)) => Boolean): Iterable[Slot] = {
    slots.filter(f).values
  }

  def get(key: String): Option[Slot] = slots.get(VariableSlotKey(key))

  def add(key: String, slot: Slot): Unit = slot match {
    case LongSlot(_, nullable, typ) => newLong(key, nullable, typ)
    case RefSlot(_, nullable, typ) => newReference(key, nullable, typ)
  }

  def copy(): SlotConfiguration = {
    val newPipeline = new SlotConfiguration(this.slots.clone(), numberOfLongs, numberOfReferences)
    slotAliases.foreach {
      case (key, aliases) =>
        newPipeline.slotAliases.put(key, mutable.Set.empty[String])
        aliases.foreach(alias => newPipeline.slotAliases.addBinding(key, alias))
    }
    newPipeline
  }

  def emptyUnderSameApply(): SlotConfiguration = {
    val applyPlanSlots = mutable.Map.empty[SlotKey, Slot]
    applyPlanSlots ++= slots.iterator.filter(kv => kv._1.isInstanceOf[ApplyPlanSlotKey])
    new SlotConfiguration(applyPlanSlots, 0, 0)
  }

  @scala.annotation.tailrec
  private def replaceExistingSlot(key: String, existingSlot: Slot, modifiedSlot: Slot): Unit = {
    if (slotAliases.contains(key)) {
      val existingAliases = slotAliases(key)
      // Propagate changes to all corresponding entries in the slots map
      slots.put(VariableSlotKey(key), modifiedSlot)
      existingAliases.foreach(alias => slots.put(VariableSlotKey(alias), modifiedSlot))
    } else {
      // Find original key
      val originalKey = slotAliases.find {
        case (_, aliases) => aliases.contains(key)
      }.map(_._1).getOrElse(throw new InternalException(s"No original key found for alias $key"))
      replaceExistingSlot(originalKey, existingSlot, modifiedSlot)
    }
  }

  private def unifyTypeAndNullability(key: String, existingSlot: Slot, newSlot: Slot): Unit = {
    val updateNullable = !existingSlot.nullable && newSlot.nullable
    val updateTyp = existingSlot.typ != newSlot.typ && !existingSlot.typ.isAssignableFrom(newSlot.typ)
    require(!updateTyp || newSlot.typ.isAssignableFrom(existingSlot.typ))
    if (updateNullable || updateTyp) {
      val modifiedSlot = (existingSlot, updateNullable, updateTyp) match {
        // We are conservative about nullability and increase it to true
        case (LongSlot(offset, _, _), true, true) =>
          LongSlot(offset, nullable = true, newSlot.typ)
        case (RefSlot(offset, _, _), true, true) =>
          RefSlot(offset, nullable = true, newSlot.typ)
        case (LongSlot(offset, _, typ), true, false) =>
          LongSlot(offset, nullable = true, typ)
        case (RefSlot(offset, _, typ), true, false) =>
          RefSlot(offset, nullable = true, typ)
        case (LongSlot(offset, nullable, _), false, true) =>
          LongSlot(offset, nullable, newSlot.typ)
        case (RefSlot(offset, nullable, _), false, true) =>
          RefSlot(offset, nullable, newSlot.typ)
        case config => throw new InternalException(s"Unexpected slot configuration: $config")
      }
      replaceExistingSlot(key, existingSlot, modifiedSlot)
    }
  }

  def newLong(key: String, nullable: Boolean, typ: CypherType): SlotConfiguration = {
    val slot = LongSlot(numberOfLongs, nullable, typ)
    slots.get(VariableSlotKey(key)) match {
      case Some(existingSlot) =>
        if (!existingSlot.isTypeCompatibleWith(slot)) {
          throw new InternalException(s"Tried overwriting already taken variable name '$key' as $slot (was: $existingSlot)")
        }
        // Reuse the existing (compatible) slot
        unifyTypeAndNullability(key, existingSlot, slot)

      case None =>
        slots.put(VariableSlotKey(key), slot)
        slotAliases.put(key, mutable.Set.empty[String])
        numberOfLongs = numberOfLongs + 1
    }
    this
  }

  def newArgument(applyPlanId: Id): SlotConfiguration = {
    if (slots.contains(ApplyPlanSlotKey(applyPlanId))) {
      throw new IllegalStateException(s"Should only add argument once per plan, got plan with $applyPlanId twice")
    }
    if (applyPlanId != Id.INVALID_ID) { // Top level argument is not allocated
      slots.put(ApplyPlanSlotKey(applyPlanId), LongSlot(numberOfLongs, false, CTAny))
      numberOfLongs = numberOfLongs + 1
    }
    this
  }

  def newReference(key: String, nullable: Boolean, typ: CypherType): SlotConfiguration = {
    val slot = RefSlot(numberOfReferences, nullable, typ)
    val slotKey = VariableSlotKey(key)
    slots.get(slotKey) match {
      case Some(existingSlot) =>
        if (!existingSlot.isTypeCompatibleWith(slot)) {
          throw new InternalException(s"Tried overwriting already taken variable name '$key' as $slot (was: $existingSlot)")
        }
        // Reuse the existing (compatible) slot
        unifyTypeAndNullability(key, existingSlot, slot)

      case None =>
        slots.put(slotKey, slot)
        slotAliases.put(key, mutable.Set.empty[String])
        numberOfReferences = numberOfReferences + 1
    }
    this
  }

  def newCachedProperty(key: ASTCachedProperty, shouldDuplicate: Boolean = false): SlotConfiguration = {
    val slotKey = CachedPropertySlotKey(key)
    slots.get(slotKey) match {
      case Some(_) =>
        // RefSlots for cached node properties are always compatible and identical in nullability and type. We can therefore reuse the existing slot.
        if (shouldDuplicate) {
          // In case we want to copy a whole bunch of slots at runtime using Arraycopy, we dont want to exclude same cached property slot,
          // even if it already exists in the row. To make that possible, we increase the number of references here, which will mean that there will be no
          // assigned slot for that array index in the references array. We can then simply copy the cached property into that position together with all
          // the other slots. We won't read it ever again from that array position, we will rather read the duplicate that exists at some other position
          // in the row.
          numberOfReferences += 1
        }

      case None =>
        slots.put(slotKey, RefSlot(numberOfReferences, nullable = false, CTAny))
        numberOfReferences = numberOfReferences + 1
    }
    this
  }

  def getReferenceOffsetFor(name: String): Int = slots.get(VariableSlotKey(name)) match {
    case Some(s: RefSlot) => s.offset
    case Some(s) => throw new InternalException(s"Uh oh... There was no reference slot for `$name`. It was a $s")
    case _ => throw new InternalException(s"Uh oh... There was no slot for `$name`")
  }

  def getLongOffsetFor(name: String): Int = slots.get(VariableSlotKey(name)) match {
    case Some(s: LongSlot) => s.offset
    case Some(s) => throw new InternalException(s"Uh oh... There was no long slot for `$name`. It was a $s")
    case _ => throw new InternalException(s"Uh oh... There was no slot for `$name`")
  }

  def getLongSlotFor(name: String): Slot = slots.get(VariableSlotKey(name)) match {
    case Some(s: LongSlot) => s
    case Some(s) => throw new InternalException(s"Uh oh... There was no long slot for `$name`. It was a $s")
    case _ => throw new InternalException(s"Uh oh... There was no slot for `$name`")
  }

  def getArgumentLongOffsetFor(applyPlanId: Id): Int = {
    if (applyPlanId == Id.INVALID_ID) {
      TopLevelArgument.SLOT_OFFSET
    } else {
      slots.getOrElse(ApplyPlanSlotKey(applyPlanId),
        throw new InternalException(s"No argument slot allocated for plan with $applyPlanId")).offset
    }
  }

  def getCachedPropertyOffsetFor(key: ASTCachedProperty): Int = slots(CachedPropertySlotKey(key)).offset

  def updateAccessorFunctions(key: String,
                              getter: CypherRow => AnyValue,
                              setter: (CypherRow, AnyValue) => Unit,
                              primitiveNodeSetter: Option[(CypherRow, Long, EntityById) => Unit],
                              primitiveRelationshipSetter: Option[(CypherRow, Long, EntityById) => Unit]): Unit = {
    getters += key -> getter
    setters += key -> setter
    primitiveNodeSetter.foreach(primitiveNodeSetters += key -> _)
    primitiveRelationshipSetter.foreach(primitiveRelationshipSetters += key -> _)
  }

  def getter(key: String): CypherRow => AnyValue = {
    getters(key)
  }

  def setter(key: String): (CypherRow, AnyValue) => Unit = {
    setters(key)
  }

  def maybeGetter(key: String): Option[CypherRow => AnyValue] = {
    getters.get(key)
  }

  def maybeSetter(key: String): Option[(CypherRow, AnyValue) => Unit] = {
    setters.get(key)
  }

  def maybePrimitiveNodeSetter(key: String): Option[(CypherRow, Long, EntityById) => Unit] = {
    primitiveNodeSetters.get(key)
  }

  def maybePrimitiveRelationshipSetter(key: String): Option[(CypherRow, Long, EntityById) => Unit] = {
    primitiveRelationshipSetters.get(key)
  }

  /**
   * Apply a function to all SlotKey->Slot tuples.
   * If there are aliases to the same Slot, the function will be applied for each SlotKey.
   */
  def foreachSlot[U](f: ((SlotKey, Slot)) => U): Unit = {
    slots.foreach(f)
  }

  /**
   * Apply a function to all SlotKey->Slot tuples.
   * The function will be applied in increasing slot offset order, first for the long slots and then for the ref slots.
   * If there are aliases to the same Slot, the function will be applied for each SlotKey.
   *
   * @param skipFirst the amount of longs and refs to be skipped in the beginning
   */
  def foreachSlotOrdered(f: ((SlotKey, Slot)) => Unit,
                         skipFirst: SlotConfiguration.Size = SlotConfiguration.Size.zero
                        ): Unit = {
    val (longs, refs) = slots.toSeq.partition(_._2.isLongSlot)

    longs.filter(_._2.offset >= skipFirst.nLongs).sortBy(_._2.offset).foreach(f)
    refs.filter(_._2.offset >= skipFirst.nReferences).sortBy(_._2.offset).foreach(f)
  }

  def foreachCachedSlot[U](onCachedProperty: ((ASTCachedProperty, RefSlot)) => Unit): Unit = {
    slots.iterator.foreach {
      case (CachedPropertySlotKey(key), slot: RefSlot) => onCachedProperty(key, slot)
      case _ => // do nothing
    }
  }

  // NOTE: This will give duplicate slots when we have aliases
  def mapSlot[U](onVariable: ((String,Slot)) => U,
                 onCachedProperty: ((ASTCachedProperty, RefSlot)) => U
                ): Iterable[U] = {
    slots.map {
      case (VariableSlotKey(key), slot) => onVariable(key, slot)
      case (CachedPropertySlotKey(key), slot: RefSlot) => onCachedProperty(key, slot)
      case (_: ApplyPlanSlotKey, slot) => throw new SlotAllocationFailed("SlotConfiguration.mapSlot does not support ApplyPlanSlots yet")
    }
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[SlotConfiguration]

  override def equals(other: Any): Boolean = other match {
    case that: SlotConfiguration =>
      (that canEqual this) &&
        slots == that.slots &&
        numberOfLongs == that.numberOfLongs &&
        numberOfReferences == that.numberOfReferences
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq[Any](slots, numberOfLongs, numberOfReferences)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"SlotConfiguration(longs=$numberOfLongs, refs=$numberOfReferences, slots=$slots)"

  def hasCachedPropertySlot(key: ASTCachedProperty): Boolean = slots.contains(CachedPropertySlotKey(key))

  def getCachedPropertySlot(key: ASTCachedProperty): Option[RefSlot] = slots.get(CachedPropertySlotKey(key)).asInstanceOf[Option[RefSlot]]
}
