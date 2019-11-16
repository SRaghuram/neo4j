/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.physicalplanning.SlotAllocation.INITIAL_SLOT_CONFIGURATION
import org.neo4j.cypher.internal.physicalplanning.{LongSlot, RefSlot, SlotConfiguration, TopLevelArgument}
import org.neo4j.cypher.internal.runtime.pipelined.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.runtime.slotted.{SlottedCompatible, SlottedExecutionContext}
import org.neo4j.cypher.internal.runtime.{EntityById, ExecutionContext, ResourceLinenumber}
import org.neo4j.cypher.internal.v4_0.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.v4_0.util.AssertionRunner
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTNode, CTRelationship}
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.NotFoundException
import org.neo4j.util.Preconditions
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.{VirtualNodeValue, VirtualRelationshipValue}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object MorselExecutionContext {
  def apply(morsel: Morsel, slots: SlotConfiguration, maxNumberOfRows: Int) =
    new MorselExecutionContext(morsel, slots, maxNumberOfRows, 0, 0, maxNumberOfRows)

  val empty: MorselExecutionContext = new MorselExecutionContext(new Morsel(Array.empty, Array.empty), SlotConfiguration.empty, 0)

  def createInitialRow(): FilteringPipelinedExecutionContext =
    new FilteringPipelinedExecutionContext(
      Morsel.create(INITIAL_SLOT_CONFIGURATION, 1),
      INITIAL_SLOT_CONFIGURATION, 1, 0, 0, 1) {

      //it is ok to asked for a cached value even though nothing is allocated for it
      override def getCachedPropertyAt(offset: Int): Value = null
      override def shallowCopy(): FilteringPipelinedExecutionContext = createInitialRow()
    }
}

class MorselExecutionContext(private[execution] final val morsel: Morsel,
                             final val slots: SlotConfiguration,
                             final val maxNumberOfRows: Int,
                             private[execution] var currentRow: Int = 0,
                             private[execution] var startRow: Int = 0,
                             private[execution] var endRow: Int = 0,
                             final val producingWorkUnitEvent: WorkUnitEvent = null) extends ExecutionContext with SlottedCompatible {
  protected final val longsPerRow: Int = slots.numberOfLongs
  protected final val refsPerRow: Int = slots.numberOfReferences

  // ====================
  // PIPELINED ATTACHMENT

  // A morsel attachment is a limited life-span attachment to a morsel, which can be
  // used to pass arbitrary data which related to a particular morsel through the
  // regular buffers. Note that only a single attachment is available at any time,
  // so designs using this functionality must ensure that no other usage can occur
  // between the intended attach and detach.

  private var attachedMorsel: MorselExecutionContext = _

  def attach(morsel: MorselExecutionContext): Unit = {
    Preconditions.checkState(
      attachedMorsel == null,
      "Cannot override existing MorselExecutionContext.attachment.")

    attachedMorsel = morsel
  }

  def detach(): MorselExecutionContext = {
    Preconditions.checkState(
      attachedMorsel != null,
      "Cannot detach if no attachment available.")

    val temp = attachedMorsel
    attachedMorsel = null
    temp
  }

  // ====================

  def shallowCopy(): MorselExecutionContext = new MorselExecutionContext(morsel, slots, maxNumberOfRows, currentRow, startRow, endRow)

  @inline def numberOfRows: Int = endRow - startRow

  @inline
  def moveToNextRow(): Unit = {
    currentRow += 1
  }

  @inline def getValidRows: Int = numberOfRows

  @inline def getFirstRow: Int = startRow

  @inline def getLastRow: Int = endRow - 1

  @inline def getCurrentRow: Int = currentRow

  @inline def getLongsPerRow: Int = longsPerRow

  @inline def getRefsPerRow: Int = refsPerRow

  @inline def setCurrentRow(row: Int): Unit = currentRow = row

  @inline def resetToFirstRow(): Unit = currentRow = startRow
  @inline def resetToBeforeFirstRow(): Unit = currentRow = startRow - 1
  @inline def setToAfterLastRow(): Unit = currentRow = endRow

  @inline def isValidRow: Boolean = currentRow >= startRow && currentRow < endRow
  @inline def hasNextRow: Boolean = currentRow < endRow - 1

  /**
    * Check if there is at least one valid row of data
    */
  @inline def hasData: Boolean = getValidRows > 0

  /**
    * Check if the morsel is empty
    */
  @inline def isEmpty: Boolean = !hasData

  /**
    * Adapt the valid rows of the morsel so that the last valid row is the previous one according to the current position.
    * This usually happens after one operator finishes writing to a morsel.
    */
  @inline def finishedWriting(): Unit = endRow = currentRow

  /**
    * Set the valid rows of the morsel to the current position of another morsel
    */
  def finishedWritingUsing(otherContext: MorselExecutionContext): Unit = {
    if (this.startRow != otherContext.startRow) {
      throw new IllegalStateException("Cannot write to a context from a context with a different first row.")
    }
    endRow = otherContext.currentRow
  }

  /**
    * @param start first index of the view (inclusive start)
    * @param end first index after the view (exclusive end)
    * @return a shallow copy that is configured to only see the configured view.
    */
  def view(start: Int, end: Int): MorselExecutionContext = {
    val view = shallowCopy()
    view.startRow = start
    view.currentRow = start
    view.endRow = end
    view
  }

  def copyRowsFrom(input: MorselExecutionContext, nInputRows: Int): Unit = {
    if (longsPerRow > 0)
      System.arraycopy(input.morsel.longs, 0, morsel.longs, startRow * longsPerRow, nInputRows * longsPerRow)
    if (refsPerRow > 0)
      System.arraycopy(input.morsel.refs, 0, morsel.refs, startRow * refsPerRow, nInputRows * refsPerRow)
  }

  override def copyTo(target: ExecutionContext, sourceLongOffset: Int = 0, sourceRefOffset: Int = 0, targetLongOffset: Int = 0, targetRefOffset: Int = 0): Unit =
    target match {
      case other: MorselExecutionContext =>
        System.arraycopy(morsel.longs, longsAtCurrentRow + sourceLongOffset, other.morsel.longs, other.longsAtCurrentRow + targetLongOffset, longsPerRow - sourceLongOffset)
        System.arraycopy(morsel.refs, refsAtCurrentRow + sourceRefOffset, other.morsel.refs, other.refsAtCurrentRow + targetRefOffset, refsPerRow - sourceRefOffset)

      case other: SlottedExecutionContext =>
        System.arraycopy(morsel.longs, longsAtCurrentRow + sourceLongOffset, other.longs, targetLongOffset, longsPerRow - sourceLongOffset)
        System.arraycopy(morsel.refs, refsAtCurrentRow + sourceRefOffset, other.refs, targetRefOffset, refsPerRow - sourceRefOffset)
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
    s"MorselExecutionContext[0x${System.identityHashCode(this).toHexString}](longsPerRow=$longsPerRow, refsPerRow=$refsPerRow, maxRows=$maxNumberOfRows, currentRow=$currentRow startRow=$startRow endRow=$endRow $prettyCurrentRow)"
  }

  def prettyCurrentRow: String =
    if (isValidRow) {
      s"longs: ${morsel.longs.slice(currentRow * longsPerRow, (currentRow + 1) * longsPerRow).mkString("[", ", ", "]")} " +
        s"refs: ${morsel.refs.slice(currentRow * refsPerRow, (currentRow + 1) * refsPerRow).mkString("[", ", ", "]")}"
    } else {
      s"<Invalid row>"
    }

  def prettyString: Seq[String] = {
    val longStrings = morsel.longs.slice(startRow*longsPerRow, numberOfRows*longsPerRow).map(String.valueOf)
    val refStrings = morsel.refs.slice(startRow*refsPerRow, numberOfRows*refsPerRow).map(String.valueOf)

    def widths(strings: Array[String], nCols: Int): Array[Int] = {
      val widths = new Array[Int](nCols)
      for {
        row <- 0 until (numberOfRows-startRow)
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
    for (row <- 0 until (numberOfRows-startRow)) {
      sb ++= (if ((startRow + row) == currentRow) " * " else "   ")

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
      addPrettyRowMarker(sb, startRow + row)
      rows += sb.result()
      sb.clear()
    }
    rows
  }

  protected def addPrettyRowMarker(sb: mutable.StringBuilder, row: Int): Unit = {}

  /**
    * Copies the whole row from input to this.
    */
  def copyFrom(input: MorselExecutionContext): Unit = copyFrom(input, input.longsPerRow, input.refsPerRow)

  override def setLongAt(offset: Int, value: Long): Unit = morsel.longs(currentRow * longsPerRow + offset) = value

  override def getLongAt(offset: Int): Long = getLongAt(currentRow, offset)

  def getLongAt(row: Int, offset: Int): Long = morsel.longs(row * longsPerRow + offset)

  def getArgumentAt(offset: Int): Long =
    if (offset == TopLevelArgument.SLOT_OFFSET) 0L
    else getLongAt(currentRow, offset)

  def setArgumentAt(offset: Int, argument: Long): Unit =
    if (offset == TopLevelArgument.SLOT_OFFSET) {
      TopLevelArgument.assertTopLevelArgument(argument)
    } else {
      setLongAt(offset, argument)
    }

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
    var usage = longsPerRow * maxNumberOfRows * 8L
    var i = startRow * refsPerRow
    while (i < ((startRow + maxNumberOfRows) * refsPerRow)) {
      val ref = morsel.refs(i)
      if (ref != null) {
        usage += morsel.refs(i).estimatedHeapUsage()
      }
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
            AssertionRunner.runUnderAssertion {
              throw new IllegalStateException(s"Tried to invalidate a cached property $cnp but no slot was found for the entity name in $slots.")
            }
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
            AssertionRunner.runUnderAssertion {
              throw new IllegalStateException(s"Tried to invalidate a cached property $crp but no slot was found for the entity name in $slots.")
            }
        }
    }
  }

  override def setLinenumber(file: String, line: Long, last: Boolean = false): Unit = fail()

  override def setLinenumber(line: Option[ResourceLinenumber]): Unit = fail()

  override def getLinenumber: Option[ResourceLinenumber] = fail()

  private[execution] def longsAtCurrentRow: Int = currentRow * longsPerRow

  private[execution] def refsAtCurrentRow: Int = currentRow * refsPerRow

  private def fail(): Nothing =
    throw new InternalException("Tried using a wrong context.")
}
