/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import org.neo4j.cypher.internal.physicalplanning.SlotAllocation.INITIAL_SLOT_CONFIGURATION
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.morsel.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.runtime.slotted.{SlottedCompatible, SlottedExecutionContext}
import org.neo4j.cypher.internal.runtime.{EntityById, ExecutionContext, ResourceLinenumber}
import org.neo4j.cypher.internal.v4_0.expressions.ASTCachedProperty
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.NotFoundException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object MorselExecutionContext {
  def apply(morsel: Morsel, numberOfLongs: Int, numberOfReferences: Int) = new MorselExecutionContext(morsel,
    numberOfLongs, numberOfReferences, 0, 0, SlotConfiguration.empty)
  def apply(morsel: Morsel, numberOfLongs: Int, numberOfReferences: Int, validRows: Int) = new MorselExecutionContext(morsel,
    numberOfLongs, numberOfReferences, validRows, 0, SlotConfiguration.empty)

  val empty: MorselExecutionContext = new MorselExecutionContext(new Morsel(Array.empty, Array.empty), 0, 0, 0, 0, SlotConfiguration.empty)

  def createInitialRow(): MorselExecutionContext =
    new MorselExecutionContext(
      Morsel.create(INITIAL_SLOT_CONFIGURATION, 1),
      INITIAL_SLOT_CONFIGURATION.numberOfLongs, INITIAL_SLOT_CONFIGURATION.numberOfReferences, 1, 0, INITIAL_SLOT_CONFIGURATION) {
      //TODO hmm...

      //it is ok to asked for a cached value even though nothing is allocated for it
      override def getCachedPropertyAt(offset: Int): Value = null
      override def shallowCopy(): MorselExecutionContext = createInitialRow()
    }
}

class MorselExecutionContext(private val morsel: Morsel,
                             private val longsPerRow: Int,
                             private val refsPerRow: Int,
                             // The number of valid rows in the morsel
                             private var validRows: Int,
                             private var currentRow: Int,
                             val slots: SlotConfiguration,
                             val producingWorkUnitEvent: WorkUnitEvent = null) extends ExecutionContext with SlottedCompatible {

  // The index of the first valid row
  private var firstRow: Int = 0

  // ARGUMENT COLUMNS

  def shallowCopy(): MorselExecutionContext = new MorselExecutionContext(morsel, longsPerRow, refsPerRow, validRows, currentRow, slots)

  def moveToNextRow(): Unit = {
    currentRow += 1
  }

  def getValidRows: Int = validRows

  def getFirstRow: Int = firstRow

  def getLastRow: Int = firstRow + validRows - 1

  def getCurrentRow: Int = currentRow

  def getLongsPerRow: Int = longsPerRow

  def getRefsPerRow: Int = refsPerRow

  def moveToRow(row: Int): Unit = currentRow = row

  def resetToFirstRow(): Unit = currentRow = firstRow
  def resetToBeforeFirstRow(): Unit = currentRow = firstRow - 1
  def setToAfterLastRow(): Unit = currentRow = getLastRow + 1

  def isValidRow: Boolean = currentRow >= firstRow && currentRow <= getLastRow
  def hasNextRow: Boolean = currentRow < getLastRow

  /**
    * Check if there is at least one valid row of data
    */
  def hasData: Boolean = validRows > 0

  /**
    * Check if the morsel is empty
    */
  def isEmpty: Boolean = !hasData

  /**
    * Adapt the valid rows of the morsel so that the last valid row is the previous one according to the current position.
    * This usually happens after one operator finishes writing to a morsel.
    */
  def finishedWriting(): Unit = validRows = currentRow - firstRow

  /**
    * Set the valid rows of the morsel to the current position of another morsel
    */
  def finishedWritingUsing(otherContext: MorselExecutionContext): Unit = validRows = {
    if (this.firstRow != otherContext.firstRow) {
      throw new IllegalStateException("Cannot write to a context from a context with a different first row.")
    }
    otherContext.currentRow - otherContext.firstRow
  }

  /**
    * @param start first index of the view (inclusive start)
    * @param end first index after the view (exclusive end)
    * @return a shallow copy that is configured to only see the configured view.
    */
  def view(start: Int, end: Int): MorselExecutionContext = {
    val view = shallowCopy()
    view.firstRow = start
    view.currentRow = start
    view.validRows = end - start
    view
  }


  def copyRowsFrom(input: MorselExecutionContext, nInputRows: Int): Unit = {
    if (longsPerRow > 0)
      System.arraycopy(input.morsel.longs, 0, morsel.longs, firstRow * longsPerRow, nInputRows * longsPerRow)
    if (refsPerRow > 0)
      System.arraycopy(input.morsel.refs, 0, morsel.refs, firstRow * refsPerRow, nInputRows * refsPerRow)
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

  override def toString: String = {
    s"MorselExecutionContext[0x${System.identityHashCode(this).toHexString}](longsPerRow=$longsPerRow, refsPerRow=$refsPerRow, validRows=$validRows, currentRow=$currentRow $prettyCurrentRow)"
  }

  def prettyCurrentRow: String =
    if (isValidRow) {
      s"longs: ${morsel.longs.slice(currentRow * longsPerRow, (currentRow + 1) * longsPerRow).mkString("[", ", ", "]")} " +
        s"refs: ${morsel.refs.slice(currentRow * refsPerRow, (currentRow + 1) * refsPerRow).mkString("[", ", ", "]")}"
    } else {
      s"<Invalid row>"
    }

  def prettyString: Seq[String] = {
    val longStrings = morsel.longs.slice(firstRow*longsPerRow, validRows*longsPerRow).map(String.valueOf)
    val refStrings = morsel.refs.slice(firstRow*refsPerRow, validRows*refsPerRow).map(String.valueOf)

    def widths(strings: Array[String], nCols: Int): Array[Int] = {
      val widths = new Array[Int](nCols)
      for {
        row <- 0 until (validRows-firstRow)
        col <- 0 until nCols
      } {
        widths(col) = math.max(widths(col), strings(row * nCols + col).length)
      }
      widths
    }

    val longWidths = widths(longStrings, longsPerRow)
    val refWidths = widths(refStrings, refsPerRow)

    val rows = new ArrayBuffer[String]
    val sb = new mutable.StringBuilder()
    for (row <- 0 until (validRows-firstRow)) {
      sb ++= (if ((firstRow + row) == currentRow) " * " else "   ")

      for (col <- 0 until longsPerRow) {
        val width = longWidths(col)
        sb ++= ("%" + width + "s").format(longStrings(row * longsPerRow + col))
        sb += ' '
      }
      sb += ' '
      sb += ' '
      for (col <- 0 until refsPerRow) {
        val width = refWidths(col)
        sb ++= ("%" + width + "s").format(refStrings(row * refsPerRow + col))
        sb += ' '
      }
      rows += sb.result()
      sb.clear()
    }
    rows
  }

  /**
    * Copies the whole row from input to this.
    */
  def copyFrom(input: MorselExecutionContext): Unit = copyFrom(input, input.longsPerRow, input.refsPerRow)

  override def setLongAt(offset: Int, value: Long): Unit = morsel.longs(currentRow * longsPerRow + offset) = value

  override def getLongAt(offset: Int): Long = getLongAt(currentRow, offset)

  def getLongAt(row: Int, offset: Int): Long = morsel.longs(row * longsPerRow + offset)

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

  override def mergeWith(other: ExecutionContext, entityById: EntityById): Unit = fail()

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

  /**
    * Total heap usage of all valid rows (can be a view, so might not be the whole morsel).
    * The reasoning behind this is that the other parts of the morsel would be part of other views in other buffers/argument states and will
    * also be accounted for.
    */
  override def estimatedHeapUsage: Long = {
    var usage = longsPerRow * validRows * 8L
    var i = firstRow * refsPerRow
    while (i < ((firstRow + validRows) * refsPerRow)) {
      usage += morsel.refs(i).estimatedHeapUsage()
      i += 1
    }
    usage
  }

  override def copyWith(key1: String, value1: AnyValue): ExecutionContext = fail()

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue): ExecutionContext = fail()

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): ExecutionContext = fail()

  override def copyWith(newEntries: Seq[(String, AnyValue)]): ExecutionContext = fail()

  override def boundEntities(materializeNode: Long => AnyValue, materializeRelationship: Long => AnyValue): Map[String, AnyValue] = fail()

  override def isNull(key: String): Boolean = fail()

  override def setCachedProperty(key: ASTCachedProperty, value: Value): Unit = fail()

  override def setCachedPropertyAt(offset: Int, value: Value): Unit = setRefAt(offset, value)

  override def getCachedProperty(key: ASTCachedProperty): Value = fail()

  override def getCachedPropertyAt(offset: Int): Value = getRefAt(offset).asInstanceOf[Value]

  override def invalidateCachedNodeProperties(node: Long): Unit = fail()

  override def invalidateCachedRelationshipProperties(rel: Long): Unit = fail()

  override def setLinenumber(file: String, line: Long, last: Boolean = false): Unit = fail()

  override def setLinenumber(line: Option[ResourceLinenumber]): Unit = fail()

  override def getLinenumber: Option[ResourceLinenumber] = fail()

  private def longsAtCurrentRow: Int = currentRow * longsPerRow

  private def refsAtCurrentRow: Int = currentRow * refsPerRow

  private def fail(): Nothing =
    throw new InternalException("Tried using a wrong context.")
}
