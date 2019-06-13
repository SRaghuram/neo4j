/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.v4_0.util.InternalException
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTAny, CypherType}
import org.neo4j.cypher.internal.runtime.EntityById
import org.neo4j.cypher.internal.v4_0.expressions.{ASTCachedProperty, CachedProperty}
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

  def addCachedPropertiesOf(other: SlotConfiguration, renames: Map[String, String]): Unit = {
    other.cachedProperties.foreach {
      case (prop:CachedProperty, _) =>
        newCachedProperty(prop)
        renames.get(prop.entityName).foreach(newName =>
          addAlias(prop.entityName, newName)
        )
    }
    other.applyPlans.foreach { case (id, slotOffset) => applyPlans.put(id, slotOffset) }
  }

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

  private def replaceExistingSlot(key: String, existingSlot: Slot, modifiedSlot: Slot): Unit = {
    val existingAliases = slotAliases.getOrElse(existingSlot,
      throw new InternalError(s"Slot allocation failure - missing slot $existingSlot for $key")
    )
    assert(existingAliases.contains(key))
    slotAliases.put(modifiedSlot, existingAliases)
    // Propagate changes to all corresponding entries in the slots map
    existingAliases.foreach(alias => slots.put(alias, modifiedSlot))
    slotAliases.remove(existingSlot)
  }

  private def unifyTypeAndNullability(key: String, existingSlot: Slot, newSlot: Slot): Unit = {
    val updateNullable = !existingSlot.nullable && newSlot.nullable
    val updateTyp = existingSlot.typ != newSlot.typ && !existingSlot.typ.isAssignableFrom(newSlot.typ)
    assert(!updateTyp || newSlot.typ.isAssignableFrom(existingSlot.typ))
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
          throw new InternalException(s"Tried overwriting already taken variable name $key as $slot (was: $existingSlot)")
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
    applyPlans.put(applyPlanId, numberOfLongs)
    numberOfLongs = numberOfLongs + 1
    this
  }

  def newReference(key: String, nullable: Boolean, typ: CypherType): SlotConfiguration = {
    val slot = RefSlot(numberOfReferences, nullable, typ)
    slots.get(key) match {
      case Some(existingSlot) =>
        if (!existingSlot.isTypeCompatibleWith(slot)) {
          throw new InternalException(s"Tried overwriting already taken variable name $key as $slot (was: $existingSlot)")
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

  def newCachedProperty(key: ASTCachedProperty): SlotConfiguration = {
    cachedProperties.get(key) match {
      case Some(_) =>
        // RefSlots for cached node properties are always compatible and identical in nullability and type. We can therefore reuse the existing slot.

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
    applyPlans.getOrElse(applyPlanId,
                         throw new InternalException(s"No argument slot allocated for plan with $applyPlanId"))
  }

  def getCachedPropertyOffsetFor(key: ASTCachedProperty): Int = cachedProperties(key).offset

  def updateAccessorFunctions(key: String, getter: ExecutionContext => AnyValue, setter: (ExecutionContext, AnyValue) => Unit,
                              primitiveNodeSetter: Option[(ExecutionContext, Long, EntityById) => Unit],
                              primitiveRelationshipSetter: Option[(ExecutionContext, Long, EntityById) => Unit]) = {
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
                         onCachedProperty: ASTCachedProperty => Unit
                        ): Unit = {
    val (longs, refs) = slots.toSeq.partition(_._2.isLongSlot)
    for ((variable, slot) <- longs.sortBy(_._2.offset)) onVariable(variable, slot)

    var sortedRefs = refs.sortBy(_._2.offset)
    var sortedCached = cachedProperties.toSeq.sortBy(_._2.offset)
    var i = 0
    while (i < numberOfReferences) {
      if (sortedRefs.nonEmpty && sortedRefs.head._2.offset == i) {
        val (variable, slot) = sortedRefs.head
        onVariable(variable, slot)
        sortedRefs = sortedRefs.tail
      } else {
        onCachedProperty(sortedCached.head._1)
        sortedCached = sortedCached.tail
      }
      i += 1
    }
  }

  def foreachCachedSlot[U](onCachedProperty: ((ASTCachedProperty, RefSlot)) => Unit): Unit = {
    cachedProperties.foreach(onCachedProperty)
  }

  // NOTE: This will give duplicate slots when we have aliases
  def mapSlot[U](f: ((String,Slot)) => U): Iterable[U] = slots.map(f)

  def partitionSlots(p: (String, Slot) => Boolean): (Seq[(String, Slot)], Seq[(String, Slot)]) = {
    slots.toSeq.partition {
      case (k, slot) =>
        p(k, slot)
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

  override def toString = s"SlotConfiguration(longs=$numberOfLongs, refs=$numberOfReferences, slots=$slots, cachedProperties=$cachedProperties)"

  /**
    * NOTE: Only use for debugging
    */
  def getLongSlots: immutable.IndexedSeq[SlotWithAliases] =
    slotAliases.toIndexedSeq.collect {
      case (slot: LongSlot, aliases) => LongSlotWithAliases(slot, aliases.toSet)
    }.sorted(SlotWithAliasesOrdering)

  /**
    * NOTE: Only use for debugging
    */
  def getRefSlots: immutable.IndexedSeq[SlotWithAliases] =
    slotAliases.toIndexedSeq.collect {
      case (slot: RefSlot, aliases) => RefSlotWithAliases(slot, aliases.toSet)
    }.sorted(SlotWithAliasesOrdering)

  /**
    * NOTE: Only use for debugging
    */
  def getCachedPropertySlots: immutable.IndexedSeq[SlotWithAliases] =
    cachedProperties.toIndexedSeq.map {
      case (cachedNodeProperty, slot) => RefSlotWithAliases(slot, Set(cachedNodeProperty.asCanonicalStringVal))
    }.sorted(SlotWithAliasesOrdering)

  def hasCachedPropertySlot(key: ASTCachedProperty): Boolean = cachedProperties.contains(key)

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
