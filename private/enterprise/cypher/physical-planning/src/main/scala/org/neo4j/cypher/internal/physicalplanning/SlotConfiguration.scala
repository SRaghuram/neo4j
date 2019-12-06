/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.Size
import org.neo4j.cypher.internal.runtime.{EntityById, ExecutionContext}
import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.{CTAny, CypherType}
import org.neo4j.exceptions.InternalException
import org.neo4j.values.AnyValue

import scala.collection.{immutable, mutable}

object SlotConfiguration {
  def empty = new SlotConfiguration(mutable.Map.empty, mutable.Map.empty, mutable.Map.empty, 0, 0)

  def apply(slots: Map[String, Slot], numberOfLongs: Int, numberOfReferences: Int): SlotConfiguration = {
    val stringToSlot = mutable.Map(slots.toSeq: _*)
    new SlotConfiguration(stringToSlot, mutable.Map.empty, mutable.Map.empty, numberOfLongs, numberOfReferences)
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
}

/**
  * A configuration which maps variables to slots. Two types of slot exists: LongSlot and RefSlot. In LongSlots we
  * store nodes and relationships, represented by their ids, and in RefSlots everything else, represented as AnyValues.
  *
  * @param slots the slots of the configuration.
  * @param numberOfLongs the number of long slots.
  * @param numberOfReferences the number of ref slots.
  */
class SlotConfiguration(private val slots: mutable.Map[String, Slot],
                        private val cachedProperties: mutable.Map[ASTCachedProperty, RefSlot],
                        private val applyPlans: mutable.Map[Id, Int],
                        var numberOfLongs: Int,
                        var numberOfReferences: Int) {

  private val aliases: mutable.Set[String] = mutable.Set()
  private val slotAliases = new mutable.HashMap[Slot, mutable.Set[String]] with mutable.MultiMap[Slot, String]

  private val getters: mutable.Map[String, ExecutionContext => AnyValue] = new mutable.HashMap[String, ExecutionContext => AnyValue]()
  private val setters: mutable.Map[String, (ExecutionContext, AnyValue) => Unit] = new mutable.HashMap[String, (ExecutionContext, AnyValue) => Unit]()
  private val primitiveNodeSetters: mutable.Map[String, (ExecutionContext, Long, EntityById) => Unit] = new mutable.HashMap[String, (ExecutionContext, Long, EntityById) => Unit]()
  private val primitiveRelationshipSetters: mutable.Map[String, (ExecutionContext, Long, EntityById) => Unit] = new mutable.HashMap[String, (ExecutionContext, Long, EntityById) => Unit]()

  def size() = SlotConfiguration.Size(numberOfLongs, numberOfReferences)

  def addAlias(newKey: String, existingKey: String): SlotConfiguration = {
    val slot = slots.getOrElse(existingKey,
      throw new SlotAllocationFailed(s"Tried to alias non-existing slot '$existingKey'  with alias '$newKey'"))
    slots.put(newKey, slot)
    aliases.add(newKey)
    slotAliases.addBinding(slot, newKey)
    this
  }

  def isAlias(key: String): Boolean = {
    aliases.contains(key)
  }

  def apply(key: String): Slot = slots.apply(key)

  def nameOfLongSlot(offset: Int): Option[String] = slots.collectFirst {
    case (name, LongSlot(o, _, _)) if o == offset && !aliases(name) => name
  }

  def filterSlots[U](onVariable: ((String,Slot)) => Boolean,
                     onCachedProperty: ((ASTCachedProperty, RefSlot)) => Boolean
                    ): Iterable[Slot] = {
    (slots.filter(onVariable) ++ cachedProperties.filter(onCachedProperty)).values
  }

  def get(key: String): Option[Slot] = slots.get(key)

  def add(key: String, slot: Slot): Unit = slot match {
    case LongSlot(_, nullable, typ) => newLong(key, nullable, typ)
    case RefSlot(_, nullable, typ) => newReference(key, nullable, typ)
  }

  def copy(): SlotConfiguration = {
    val newPipeline = new SlotConfiguration(this.slots.clone(),
                                            this.cachedProperties.clone(),
                                            this.applyPlans.clone(),
                                            numberOfLongs,
                                            numberOfReferences)
    newPipeline.aliases ++= aliases
    newPipeline.slotAliases ++= slotAliases
    newPipeline
  }

  def emptyUnderSameApply(): SlotConfiguration = {
    new SlotConfiguration(mutable.Map.empty,
      mutable.Map.empty,
      this.applyPlans.clone(),
      0,
      0)
  }

  private def replaceExistingSlot(key: String, existingSlot: Slot, modifiedSlot: Slot): Unit = {
    val existingAliases = slotAliases.getOrElse(existingSlot,
      throw new InternalError(s"Slot allocation failure - missing slot $existingSlot for $key")
    )
    require(existingAliases.contains(key))
    slotAliases.put(modifiedSlot, existingAliases)
    // Propagate changes to all corresponding entries in the slots map
    existingAliases.foreach(alias => slots.put(alias, modifiedSlot))
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
        case config => throw new InternalException(s"Unxpected slot configuration: $config")
      }
      replaceExistingSlot(key, existingSlot, modifiedSlot)
    }
  }

  def newLong(key: String, nullable: Boolean, typ: CypherType): SlotConfiguration = {
    val slot = LongSlot(numberOfLongs, nullable, typ)
    slots.get(key) match {
      case Some(existingSlot) =>
        if (!existingSlot.isTypeCompatibleWith(slot)) {
          throw new InternalException(s"Tried overwriting already taken variable name '$key' as $slot (was: $existingSlot)")
        }
        // Reuse the existing (compatible) slot
        unifyTypeAndNullability(key, existingSlot, slot)

      case None =>
        slots.put(key, slot)
        slotAliases.addBinding(slot, key)
        numberOfLongs = numberOfLongs + 1
    }
    this
  }

  def newArgument(applyPlanId: Id): SlotConfiguration = {
    if (applyPlans.contains(applyPlanId)) {
      throw new IllegalStateException(s"Should only add argument once per plan, got plan with $applyPlanId twice")
    }
    if (applyPlanId != Id.INVALID_ID) { // Top level argument is not allocated
      applyPlans.put(applyPlanId, numberOfLongs)
      numberOfLongs = numberOfLongs + 1
    }
    this
  }

  def newReference(key: String, nullable: Boolean, typ: CypherType): SlotConfiguration = {
    val slot = RefSlot(numberOfReferences, nullable, typ)
    slots.get(key) match {
      case Some(existingSlot) =>
        if (!existingSlot.isTypeCompatibleWith(slot)) {
          throw new InternalException(s"Tried overwriting already taken variable name '$key' as $slot (was: $existingSlot)")
        }
        // Reuse the existing (compatible) slot
        unifyTypeAndNullability(key, existingSlot, slot)

      case None =>
        slots.put(key, slot)
        slotAliases.addBinding(slot, key)
        numberOfReferences = numberOfReferences + 1
    }
    this
  }

  def newCachedProperty(key: ASTCachedProperty, shouldDuplicate: Boolean = false): SlotConfiguration = {
    cachedProperties.get(key) match {
      case Some(_) =>
        // RefSlots for cached node properties are always compatible and identical in nullability and type. We can therefore reuse the existing slot.
        if (shouldDuplicate) {
          numberOfReferences += 1
        }

      case None =>
        cachedProperties.put(key, RefSlot(numberOfReferences, nullable = false, CTAny))
        numberOfReferences = numberOfReferences + 1
    }
    this
  }

  def getReferenceOffsetFor(name: String): Int = slots.get(name) match {
    case Some(s: RefSlot) => s.offset
    case Some(s) => throw new InternalException(s"Uh oh... There was no reference slot for `$name`. It was a $s")
    case _ => throw new InternalException(s"Uh oh... There was no slot for `$name`")
  }

  def getLongOffsetFor(name: String): Int = slots.get(name) match {
    case Some(s: LongSlot) => s.offset
    case Some(s) => throw new InternalException(s"Uh oh... There was no long slot for `$name`. It was a $s")
    case _ => throw new InternalException(s"Uh oh... There was no slot for `$name`")
  }

  def getLongSlotFor(name: String): Slot = slots.get(name) match {
    case Some(s: LongSlot) => s
    case Some(s) => throw new InternalException(s"Uh oh... There was no long slot for `$name`. It was a $s")
    case _ => throw new InternalException(s"Uh oh... There was no slot for `$name`")
  }

  def getArgumentLongOffsetFor(applyPlanId: Id): Int = {
    if (applyPlanId == Id.INVALID_ID) {
      TopLevelArgument.SLOT_OFFSET
    } else {
      applyPlans.getOrElse(applyPlanId,
                           throw new InternalException(s"No argument slot allocated for plan with $applyPlanId"))
    }
  }

  def getCachedPropertyOffsetFor(key: ASTCachedProperty): Int = cachedProperties(key).offset

  def updateAccessorFunctions(key: String, getter: ExecutionContext => AnyValue, setter: (ExecutionContext, AnyValue) => Unit,
                              primitiveNodeSetter: Option[(ExecutionContext, Long, EntityById) => Unit],
                              primitiveRelationshipSetter: Option[(ExecutionContext, Long, EntityById) => Unit]): Unit = {
    getters += key -> getter
    setters += key -> setter
    primitiveNodeSetter.map(primitiveNodeSetters += key -> _)
    primitiveRelationshipSetter.map(primitiveRelationshipSetters += key -> _)
  }

  def getter(key: String): ExecutionContext => AnyValue = {
    getters(key)
  }

  def setter(key: String): (ExecutionContext, AnyValue) => Unit = {
    setters(key)
  }

  def maybeGetter(key: String): Option[ExecutionContext => AnyValue] = {
    getters.get(key)
  }

  def maybeSetter(key: String): Option[(ExecutionContext, AnyValue) => Unit] = {
    setters.get(key)
  }

  def maybePrimitiveNodeSetter(key: String): Option[(ExecutionContext, Long, EntityById) => Unit] = {
    primitiveNodeSetters.get(key)
  }

  def maybePrimitiveRelationshipSetter(key: String): Option[(ExecutionContext, Long, EntityById) => Unit] = {
    primitiveRelationshipSetters.get(key)
  }

  // NOTE: This will give duplicate slots when we have aliases
  def foreachSlot[U](onVariable: ((String, Slot)) => U,
                     onCachedProperty: ((ASTCachedProperty, RefSlot)) => Unit
                    ): Unit = {
    slots.foreach(onVariable)
    cachedProperties.foreach(onCachedProperty)
  }

  // NOTE: This will give duplicate slots when we have aliases
  def foreachSlotOrdered(onVariable: (String, Slot) => Unit,
                         onCachedProperty: ASTCachedProperty => Unit,
                         onApplyPlan: Id => Unit = _ => (),
                         skipFirst: Size = Size.zero
                        ): Unit = {
    val (longs, refs) = slots.toSeq.partition(_._2.isLongSlot)

    var sortedRefs: Seq[(String, Slot)] = refs.filter(_._2.offset >= skipFirst.nReferences).sortBy(_._2.offset)
    var sortedLongs: Seq[(String, Slot)] = longs.filter(_._2.offset >= skipFirst.nLongs).sortBy(_._2.offset)
    var sortedCached: Seq[(ASTCachedProperty, RefSlot)] = cachedProperties.toSeq.filter(_._2.offset >= skipFirst.nReferences).sortBy(_._2.offset)
    var sortedApplyPlanIds: Seq[(Id, Int)] = applyPlans.toSeq.filter(_._2 >= skipFirst.nLongs).sortBy(_._2)

    def _onVariable(tuple: (String, Slot)): Unit = {
      val (variable, slot) = tuple
      if (slot.isLongSlot) {
        onVariable(variable, slot)
        sortedLongs = sortedLongs.tail
      } else {
        onVariable(variable, slot)
        sortedRefs = sortedRefs.tail
      }
    }

    def _onCached(tuple: (ASTCachedProperty, RefSlot)): Unit = {
      val (cached, _) = tuple
      onCachedProperty(cached)
      sortedCached = sortedCached.tail
    }

    def _onApplyPlanId(tuple: (Id, Int)): Unit = {
      val (id, _) = tuple
      onApplyPlan(id)
      sortedApplyPlanIds = sortedApplyPlanIds.tail
    }

    while (sortedRefs.nonEmpty || sortedCached.nonEmpty) {
      (sortedRefs.headOption, sortedCached.headOption) match {
        case (Some(ref), None) => _onVariable(ref)
        case (None, Some(cached)) => _onCached(cached)
        case (Some(ref), Some(cached)) if ref._2.offset < cached._2.offset => _onVariable(ref)
        case (Some(ref), Some(cached)) if ref._2.offset > cached._2.offset => _onCached(cached)
      }
    }

    while (sortedLongs.nonEmpty || sortedApplyPlanIds.nonEmpty) {
      (sortedLongs.headOption, sortedApplyPlanIds.headOption) match {
        case (Some(long), None) => _onVariable(long)
        case (None, Some(applyPlanId)) => _onApplyPlanId(applyPlanId)
        case (Some(long), Some(applyPlanId)) if long._2.offset < applyPlanId._2 => _onVariable(long)
        case (Some(long), Some(applyPlanId)) if long._2.offset > applyPlanId._2 => _onApplyPlanId(applyPlanId)
      }
    }
  }

  def foreachCachedSlot[U](onCachedProperty: ((ASTCachedProperty, RefSlot)) => Unit): Unit = {
    cachedProperties.foreach(onCachedProperty)
  }

  // NOTE: This will give duplicate slots when we have aliases
  def mapSlot[U](onVariable: ((String,Slot)) => U,
                 onCachedProperty: ((ASTCachedProperty, RefSlot)) => U
                ): Iterable[U] = {
    slots.map(onVariable) ++ cachedProperties.map(onCachedProperty)
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

  override def toString = s"SlotConfiguration(longs=$numberOfLongs, refs=$numberOfReferences, slots=$slots, cachedProperties=$cachedProperties)"

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
    cachedProperties.toIndexedSeq.map {
      case (cachedNodeProperty, slot) => RefSlotWithAliases(slot, Set(cachedNodeProperty.asCanonicalStringVal))
    }.sorted(SlotWithAliasesOrdering)

  def hasCachedPropertySlot(key: ASTCachedProperty): Boolean = cachedProperties.contains(key)

  def getCachedPropertySlot(key: ASTCachedProperty): Option[RefSlot] = cachedProperties.get(key)

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
