/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.slotted.{SlottedCompatible, SlottedExecutionContext}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, ResourceLinenumber}
import org.neo4j.cypher.internal.v4_0.logical.plans.CachedNodeProperty
import org.neo4j.cypher.internal.v4_0.util.InternalException
import org.neo4j.graphdb.NotFoundException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value

object MorselExecutionContext {
  def apply(morsel: Morsel, pipeline: Pipeline) = new MorselExecutionContext(morsel,
    pipeline.slots.numberOfLongs, pipeline.slots.numberOfReferences, 0, 0, pipeline.slots)
  def apply(morsel: Morsel, numberOfLongs: Int, numberOfReferences: Int) = new MorselExecutionContext(morsel,
    numberOfLongs, numberOfReferences, 0, 0, SlotConfiguration.empty)
  def apply(morsel: Morsel, numberOfLongs: Int, numberOfReferences: Int, validRows: Int) = new MorselExecutionContext(morsel,
    numberOfLongs, numberOfReferences, validRows, 0, SlotConfiguration.empty)
  private val EMPTY_SINGLE_ROW = new MorselExecutionContext(Morsel.create(SlotConfiguration.empty, 1), 0, 0, 1, 0, SlotConfiguration.empty)

  def createSingleRow(): MorselExecutionContext =
    EMPTY_SINGLE_ROW.shallowCopy()
}

class MorselExecutionContext(private val morsel: Morsel,
                             private val longsPerRow: Int,
                             private val refsPerRow: Int,
                             private var validRows: Int,
                             private var currentRow: Int,
                             val slots: SlotConfiguration) extends ExecutionContext with SlottedCompatible {

  def shallowCopy(): MorselExecutionContext = new MorselExecutionContext(morsel, longsPerRow, refsPerRow, validRows, currentRow, slots)

  def moveToNextRow(): Unit = {
    currentRow += 1
  }

  def getValidRows: Int = validRows

  def getCurrentRow: Int = currentRow

  def getLongsPerRow: Int = longsPerRow

  def getRefsPerRow: Int = refsPerRow

  def moveToRow(row: Int): Unit = currentRow = row

  def resetToFirstRow(): Unit = currentRow = 0
  def resetToBeforeFirstRow(): Unit = currentRow = -1
  def setToAfterLastRow(): Unit = currentRow = validRows

  def isValidRow: Boolean = currentRow < validRows
  def hasNextRow: Boolean = currentRow + 1 < validRows

  def numberOfRows: Int = validRows

  /**
    * Check so that there is at least one valid row of data
    */
  def hasData: Boolean = validRows > 0

  /**
    * Set the valid rows of the morsel to the current position, which usually
    * happens after one operator finishes writing to a morsel.
    */
  def finishedWriting(): Unit = validRows = currentRow

  /**
    * Set the valid rows of the morsel to the current position of another morsel
    */
  def finishedWritingUsing(otherContext: MorselExecutionContext): Unit = validRows = otherContext.currentRow

  def createViewOfCurrentRow(): MorselExecutionContext = {
    val view = shallowCopy()
    view.limitToCurrentRow()
    view
  }

  private def limitToCurrentRow(): Unit = {
    validRows =
      if (currentRow < validRows)
        currentRow+1
      else
        currentRow
  }

  def copyAllRowsFrom(input: ExecutionContext): Unit = input match {
    case other:MorselExecutionContext =>
      System.arraycopy(other.morsel.longs, 0, morsel.longs, 0, other.morsel.longs.length)
      System.arraycopy(other.morsel.refs, 0, morsel.refs, 0, other.morsel.refs.length)
    case _ => fail()
  }

  override def copyTo(target: ExecutionContext, fromLongOffset: Int = 0, fromRefOffset: Int = 0, toLongOffset: Int = 0, toRefOffset: Int = 0): Unit =
    target match {
      case other: MorselExecutionContext =>
        System.arraycopy(morsel.longs, longsAtCurrentRow + fromLongOffset, other.morsel.longs, other.longsAtCurrentRow + toLongOffset, longsPerRow - fromLongOffset)
        System.arraycopy(morsel.refs, refsAtCurrentRow + fromRefOffset, other.morsel.refs, other.refsAtCurrentRow + toRefOffset, refsPerRow - fromRefOffset)

      case other: SlottedExecutionContext =>
        System.arraycopy(morsel.longs, longsAtCurrentRow + fromLongOffset, other.longs, toLongOffset, longsPerRow - fromLongOffset)
        System.arraycopy(morsel.refs, refsAtCurrentRow + fromRefOffset, other.refs, toRefOffset, refsPerRow - fromRefOffset)
    }

  override def copyFrom(input: ExecutionContext, nLongs: Int, nRefs: Int): Unit = input match {
    case other:MorselExecutionContext =>
      if (nLongs > longsPerRow || nRefs > refsPerRow)
        throw new InternalException("A bug has occurred in the morsel runtime: The target morsel execution context cannot hold the data to copy.")
      else {
        System.arraycopy(other.morsel.longs, other.longsAtCurrentRow, morsel.longs, longsAtCurrentRow, nLongs)
        System.arraycopy(other.morsel.refs, other.refsAtCurrentRow, morsel.refs, refsAtCurrentRow, nRefs)
      }

    case other:SlottedExecutionContext =>
      if (nLongs > longsPerRow || nRefs > refsPerRow)
        throw new InternalException("A bug has occurred in the morsel runtime: The target morsel execution context cannot hold the data to copy.")
      else {
        System.arraycopy(other.longs, 0, morsel.longs, longsAtCurrentRow, nLongs)
        System.arraycopy(other.refs, 0, morsel.refs, refsAtCurrentRow, nRefs)
      }
    case _ => fail()
  }

  override def copyToSlottedExecutionContext(other: SlottedExecutionContext, nLongs: Int, nRefs: Int): Unit = {
    System.arraycopy(morsel.longs, longsAtCurrentRow, other.longs, 0, nLongs)
    System.arraycopy(morsel.refs, refsAtCurrentRow, other.refs, 0, nRefs)
  }

  override def copyCachedFrom(input: ExecutionContext): Unit = fail()

  override def toString(): String = {
    s"MorselExecutionContext[0x${System.identityHashCode(this).toHexString}]($morsel, longsPerRow=$longsPerRow, refsPerRow=$refsPerRow, validRows=$validRows, currentRow=$currentRow)"
  }

  /**
    * Copies the whole row from input to this.
    */
  def copyFrom(input: MorselExecutionContext): Unit = copyFrom(input, input.longsPerRow, input.refsPerRow)

  override def setLongAt(offset: Int, value: Long): Unit = morsel.longs(currentRow * longsPerRow + offset) = value

  override def getLongAt(offset: Int): Long = morsel.longs(currentRow * longsPerRow + offset)

  override def setRefAt(offset: Int, value: AnyValue): Unit = morsel.refs(currentRow * refsPerRow + offset) = value

  override def getRefAt(offset: Int): AnyValue = morsel.refs(currentRow * refsPerRow + offset)

  override def getByName(name: String): AnyValue = slots.maybeGetter(name).map(g => g(this)).getOrElse(throw new NotFoundException(s"Unknown variable `$name`."))

  override def containsName(name: String): Boolean = slots.maybeGetter(name).map(g => g(this)).isDefined

  override def numberOfColumns: Int = longsPerRow + refsPerRow

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

  private def setValue(key1: String, value1: AnyValue): Unit = {
    slots.maybeSetter(key1)
      .getOrElse(throw new InternalException(s"Ouch, no suitable slot for key $key1 = $value1\nSlots: $slots"))
      .apply(this, value1)
  }

  override def mergeWith(other: ExecutionContext): Unit = fail()

  override def createClone(): ExecutionContext = {
    // This is used by some expressions with the expectation of being able to overwrite an
    // identifier inside a nested scope, without affecting the data in the original row.
    // That would not work with a shallow copy, so here we make a copy of the data of
    // the current row inside the morsel.
    // (If you just need a copy of the view/iteration state of the morsel, use `shallowCopy()` instead)
    val slottedRow = SlottedExecutionContext(slots)
    copyTo(slottedRow)
    slottedRow
  }

  override def copyWith(key1: String, value1: AnyValue): ExecutionContext = fail()

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue): ExecutionContext = fail()

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): ExecutionContext = fail()

  override def copyWith(newEntries: Seq[(String, AnyValue)]): ExecutionContext = fail()

  override def boundEntities(materializeNode: Long => AnyValue, materializeRelationship: Long => AnyValue): Map[String, AnyValue] = fail()

  override def isNull(key: String): Boolean = fail()

  override def setCachedProperty(key: CachedNodeProperty, value: Value): Unit = fail()

  override def setCachedPropertyAt(offset: Int, value: Value): Unit = setRefAt(offset, value)

  override def getCachedProperty(key: CachedNodeProperty): Value = fail()

  override def getCachedPropertyAt(offset: Int): Value = getRefAt(offset).asInstanceOf[Value]

  override def invalidateCachedProperties(node: Long): Unit = fail()

  override def setLinenumber(file: String, line: Long, last: Boolean = false): Unit = fail()

  override def setLinenumber(line: Option[ResourceLinenumber]): Unit = fail()

  override def getLinenumber: Option[ResourceLinenumber] = fail()

  private def longsAtCurrentRow: Int = currentRow * longsPerRow

  private def refsAtCurrentRow: Int = currentRow * refsPerRow

  private def fail(): Nothing =
    throw new InternalException("Tried using a wrong context.")
}
