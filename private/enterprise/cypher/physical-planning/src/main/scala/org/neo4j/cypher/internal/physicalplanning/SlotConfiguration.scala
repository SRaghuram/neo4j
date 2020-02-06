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
import org.neo4j.cypher.internal.runtime.EntityById
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.exceptions.InternalException
import org.neo4j.values.AnyValue

import scala.collection.immutable
import scala.collection.mutable

object SlotConfiguration {
  def empty = new SlotConfiguration(mutable.Map.empty, 0, 0)

  def apply(slots: Map[String, Slot], numberOfLongs: Int, numberOfReferences: Int): SlotConfiguration = {
    val stringToSlot = mutable.Map[SlotKey, Slot](slots.toSeq.map(kv => (VariableSlotKey(kv._1), kv._2)): _*)
    new SlotConfiguration(stringToSlot, numberOfLongs, numberOfReferences)
  }

  def isLongSlot(slot: Slot): Boolean = slot match {
    case _: LongSlot => true
    case _ => false
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
class SlotConfiguration(private val slots: mutable.Map[SlotConfiguration.SlotKey, Slot],
                        var numberOfLongs: Int,
                        var numberOfReferences: Int) {

  private val aliases: mutable.Set[String] = mutable.Set()
  private val slotAliases = new mutable.HashMap[Slot, mutable.Set[String]] with mutable.MultiMap[Slot, String]

  private val getters: mutable.Map[String, CypherRow => AnyValue] = new mutable.HashMap[String, CypherRow => AnyValue]()
  private val setters: mutable.Map[String, (CypherRow, AnyValue) => Unit] = new mutable.HashMap[String, (CypherRow, AnyValue) => Unit]()
  private val primitiveNodeSetters: mutable.Map[String, (CypherRow, Long, EntityById) => Unit] = new mutable.HashMap[String, (CypherRow, Long, EntityById) => Unit]()
  private val primitiveRelationshipSetters: mutable.Map[String, (CypherRow, Long, EntityById) => Unit] = new mutable.HashMap[String, (CypherRow, Long, EntityById) => Unit]()

  def size() = SlotConfiguration.Size(numberOfLongs, numberOfReferences)

  def addAlias(newKey: String, existingKey: String): SlotConfiguration = {
    val slot = slots.getOrElse(VariableSlotKey(existingKey),
      throw new SlotAllocationFailed(s"Tried to alias non-existing slot '$existingKey'  with alias '$newKey'"))
    slots.put(VariableSlotKey(newKey), slot)
    aliases.add(newKey)
    slotAliases.addBinding(slot, newKey)
    this
  }

  def isAlias(key: String): Boolean = {
    aliases.contains(key)
  }

  def apply(key: String): Slot = slots.apply(VariableSlotKey(key))

  def nameOfLongSlot(offset: Int): Option[String] = slots.collectFirst {
    case (VariableSlotKey(name), LongSlot(o, _, _)) if o == offset && !aliases(name) => name
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
    newPipeline.aliases ++= aliases
    newPipeline.slotAliases ++= slotAliases
    newPipeline
  }

  def emptyUnderSameApply(): SlotConfiguration = {
    val applyPlanSlots = mutable.Map.empty[SlotKey, Slot]
    applyPlanSlots ++= slots.iterator.filter(kv => kv._1.isInstanceOf[ApplyPlanSlotKey])
    new SlotConfiguration(applyPlanSlots, 0, 0)
  }

  private def replaceExistingSlot(key: String, existingSlot: Slot, modifiedSlot: Slot): Unit = {
    val existingAliases = slotAliases.getOrElse(existingSlot,
      throw new InternalError(s"Slot allocation failure - missing slot $existingSlot for $key")
    )
    require(existingAliases.contains(key))
    slotAliases.put(modifiedSlot, existingAliases)
    // Propagate changes to all corresponding entries in the slots map
    existingAliases.foreach(alias => slots.put(VariableSlotKey(alias), modifiedSlot))
    slotAliases.remove(existingSlot)
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
        slotAliases.addBinding(slot, key)
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
        slotAliases.addBinding(slot, key)
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

  def updateAccessorFunctions(key: String, getter: CypherRow => AnyValue, setter: (CypherRow, AnyValue) => Unit,
                              primitiveNodeSetter: Option[(CypherRow, Long, EntityById) => Unit],
                              primitiveRelationshipSetter: Option[(CypherRow, Long, EntityById) => Unit]): Unit = {
    getters += key -> getter
    setters += key -> setter
    primitiveNodeSetter.map(primitiveNodeSetters += key -> _)
    primitiveRelationshipSetter.map(primitiveRelationshipSetters += key -> _)
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

  // NOTE: This will give duplicate slots when we have aliases
  def foreachSlot[U](f: ((SlotKey, Slot)) => U): Unit = {
    slots.foreach(f)
  }

  // NOTE: This will give duplicate slots when we have aliases
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

  /**
   * NOTE: Only use for debugging
   */
  def getLongSlots: immutable.IndexedSeq[SlotWithAliases] =
    slotAliases.toIndexedSeq.collect {
      case (slot: LongSlot, aliasesForSlot) => LongSlotWithAliases(slot, aliasesForSlot.toSet)
    }.sorted(SlotWithAliasesOrdering)

  /**
   * NOTE: Only use for debugging
   */
  def getRefSlots: immutable.IndexedSeq[SlotWithAliases] =
    slotAliases.toIndexedSeq.collect {
      case (slot: RefSlot, aliasesForSlot) => RefSlotWithAliases(slot, aliasesForSlot.toSet)
    }.sorted(SlotWithAliasesOrdering)

  /**
   * NOTE: Only use for debugging
   */
  def getCachedPropertySlots: immutable.IndexedSeq[SlotWithAliases] =
    slots.toIndexedSeq.collect {
      case (CachedPropertySlotKey(cachedProperty), slot: RefSlot) => RefSlotWithAliases(slot, Set(cachedProperty.asCanonicalStringVal))
    }.sorted(SlotWithAliasesOrdering)

  def hasCachedPropertySlot(key: ASTCachedProperty): Boolean = slots.contains(CachedPropertySlotKey(key))

  def getCachedPropertySlot(key: ASTCachedProperty): Option[RefSlot] = slots.get(CachedPropertySlotKey(key)).asInstanceOf[Option[RefSlot]]

  object SlotWithAliasesOrdering extends Ordering[SlotWithAliases] {
    def compare(x: SlotWithAliases, y: SlotWithAliases): Int = (x, y) match {
      case (_: LongSlotWithAliases, _: RefSlotWithAliases) =>
        -1
      case (_: RefSlotWithAliases, _: LongSlotWithAliases) =>
        1
      case _ =>
        x.slot.offset - y.slot.offset
    }
  }

  object SlotOrdering extends Ordering[Slot] {
    def compare(x: Slot, y: Slot): Int = (x, y) match {
      case (_: LongSlot, _: RefSlot) =>
        -1
      case (_: RefSlot, _: LongSlot) =>
        1
      case _ =>
        x.offset - y.offset
    }
  }
}
