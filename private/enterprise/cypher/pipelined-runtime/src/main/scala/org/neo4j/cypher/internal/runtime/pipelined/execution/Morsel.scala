/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.SlotAllocation.INITIAL_SLOT_CONFIGURATION
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.MemoizingMeasurable
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.ResourceLinenumber
import org.neo4j.cypher.internal.runtime.pipelined.state.StandardAggregators.SHALLOW_SIZE
import org.neo4j.cypher.internal.runtime.pipelined.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.NotFoundException
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstance
import org.neo4j.util.Preconditions
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Morsel {
  final val INSTANCE_SIZE = shallowSizeOfInstance(classOf[Morsel])

  val empty: Morsel = new Morsel(Array.empty, Array.empty, SlotConfiguration.empty, 0)

  def createInitialRow(): FilteringMorsel =
    new FilteringMorsel(
      new Array[Long](INITIAL_SLOT_CONFIGURATION.numberOfLongs * 1),
      new Array[AnyValue](INITIAL_SLOT_CONFIGURATION.numberOfReferences * 1),
      INITIAL_SLOT_CONFIGURATION, 1, 0, 1) {

      //it is ok to asked for a cached value even though nothing is allocated for it
      override def shallowCopy(): FilteringMorsel = createInitialRow()
    }
}

//noinspection NameBooleanParameters
class Morsel(private[execution] final val longs: Array[Long],
             private[execution] final val refs: Array[AnyValue],
             final val slots: SlotConfiguration,
             final val maxNumberOfRows: Int,
             private[execution] var startRow: Int = 0,
             private[execution] var endRow: Int = 0,
             final val producingWorkUnitEvent: WorkUnitEvent = null) extends MemoizingMeasurable {

  morselSelf =>

  final val longsPerRow: Int = slots.numberOfLongs
  final val refsPerRow: Int = slots.numberOfReferences

  // ====================
  // PIPELINED ATTACHMENT

  // A morsel attachment is a limited life-span attachment to a morsel, which can be
  // used to pass arbitrary data which related to a particular morsel through the
  // regular buffers. Note that only a single attachment is available at any time,
  // so designs using this functionality must ensure that no other usage can occur
  // between the intended attach and detach.

  private var attachedMorsel: Morsel = _

  def attach(morsel: Morsel): Unit = {
    Preconditions.checkState(
      attachedMorsel == null,
      "Cannot override existing MorselExecutionContext.attachment.")

    attachedMorsel = morsel
  }

  def detach(): Morsel = {
    Preconditions.checkState(
      attachedMorsel != null,
      "Cannot detach if no attachment available.")

    val temp = attachedMorsel
    attachedMorsel = null
    temp
  }

  // ====================

  def readCursor(onFirstRow: Boolean = false): MorselReadCursor = newCursor(onFirstRow)

  def writeCursor(onFirstRow: Boolean = false): MorselWriteCursor = newCursor(onFirstRow)

  def fullCursor(onFirstRow: Boolean = false): MorselFullCursor = newCursor(onFirstRow)

  protected def newCursor(onFirstRow: Boolean): Cursor = new Cursor(if (onFirstRow) startRow else startRow - 1)

  class Cursor(initialRow: Int) extends CypherRowAdapter with MorselFullCursor {

    protected var currentRow: Int = initialRow

    override def row: Int = currentRow

    override def setRow(row: Int): Unit = currentRow = row

    override def setToStart(): Unit = currentRow = startRow - 1

    override def setToEnd(): Unit = currentRow = endRow

    override def onValidRow(): Boolean = {
      val _currentRow = currentRow  // Accessing a protected var is a virtual call. We should avoid doing it multiple times.
      _currentRow >= startRow && _currentRow < endRow
    }

    override def next(): Boolean = {
      currentRow += 1
      currentRow < endRow
    }

    override def hasNext: Boolean = currentRow < endRow - 1

    override def truncate(): Unit = morselSelf.truncateToRow(currentRow)

    override def snapshot(): MorselRow = new Cursor(currentRow)

    override def getLongAt(offset: Int): Long = longs(currentRow * longsPerRow + offset)

    override def getRefAt(offset: Int): AnyValue = refs(currentRow * refsPerRow + offset)

    override def getByName(name: String): AnyValue = slots.maybeGetter(name).map(g => g(this)).getOrElse(throw new NotFoundException(s"Unknown variable `$name`."))

    override def getCachedPropertyAt(offset: Int): Value = getRefAt(offset).asInstanceOf[Value]

    override def setCachedPropertyAt(offset: Int, value: Value): Unit = setRefAt(offset, value)

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
            // This case is possible to reach, when we allocate a cached property before a pipeline break and before the variable it is referencing.
            // We will never evaluate that cached property in this row, and we could improve SlotAllocation to allocate it only on the next pipeline
            // instead, but that is difficult. It is harmless if we get here, we will simply not do anything.
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
            // This case is possible to reach, when we allocate a cached property before a pipeline break and before the variable it is referencing.
            // We will never evaluate that cached property in this row, and we could improve SlotAllocation to allocate it only on the next pipeline
            // instead, but that is difficult. It is harmless if we get here, we will simply not do anything.
          }
      }
    }

    override def setLongAt(offset: Int, value: Long): Unit = longs(currentRow * longsPerRow + offset) = value

    override def setRefAt(offset: Int, value: AnyValue): Unit = refs(currentRow * refsPerRow + offset) = value

    override def set(key: String,
                     value: AnyValue): Unit = {
      slots.setter(key)(this, value)
    }

    override def set(key1: String,
                     value1: AnyValue,
                     key2: String,
                     value2: AnyValue): Unit = {
      slots.setter(key1)(this, value1)
      slots.setter(key2)(this, value2)
    }

    override def set(key1: String,
                     value1: AnyValue,
                     key2: String,
                     value2: AnyValue,
                     key3: String,
                     value3: AnyValue): Unit = {
      slots.setter(key1)(this, value1)
      slots.setter(key2)(this, value2)
      slots.setter(key3)(this, value3)
    }

    override def copyAllFrom(from: ReadableRow): Unit = from match {
      case other: MorselDerivedRow =>
        copyFromMorselRow(other, 0, 0, 0, 0, other.morsel.longsPerRow, other.morsel.refsPerRow)
      case other: SlottedRow =>
        copyFromSlottedRow(other, 0, 0, 0, 0, other.slots.numberOfLongs, other.slots.numberOfReferences)
      case _ => fail()
    }

    override def copyFrom(from: ReadableRow, nLongs: Int, nRefs: Int): Unit = from match {
      case other: MorselDerivedRow =>
        copyFromMorselRow(other, 0, 0, 0, 0, nLongs, nRefs)
      case other: SlottedRow =>
        copyFromSlottedRow(other, 0, 0, 0, 0, nLongs, nRefs)
      case _ => fail()
    }

    override def copyFromOffset(from: ReadableRow,
                                fromLongOffset: Int,
                                fromRefOffset: Int,
                                thisLongOffset: Int,
                                thisRefOffset: Int): Unit =
      from match {
        case other: MorselDerivedRow =>
          val nLongs = other.morsel.longsPerRow - fromLongOffset
          val nRefs = other.morsel.refsPerRow - fromRefOffset
          copyFromMorselRow(other, fromLongOffset, fromRefOffset, thisLongOffset, thisRefOffset, nLongs, nRefs)

        case other: SlottedRow =>
          val nLongs = other.slots.numberOfLongs - fromLongOffset
          val nRefs = other.slots.numberOfReferences - fromRefOffset
          copyFromSlottedRow(other, fromLongOffset, fromRefOffset, thisLongOffset, thisRefOffset, nLongs, nRefs)
      }

    override def copyFrom(from: MorselDerivedRow): Unit = copyFromMorselRow(from, 0, 0, 0, 0, from.morsel.longsPerRow, from.morsel.refsPerRow)

    override def copyFromSlottedRowOrCursor(from: ReadableRow): Unit = from match {
      case other: SlottedRow =>
        copyFromSlottedRow(other, 0, 0, 0, 0, other.longs.length, other.refs.length)
      case other: MorselRow =>
        copyFromMorselRow(other, 0, 0, 0, 0, other.morsel.longsPerRow, other.morsel.refsPerRow)
      case _ => fail()
    }

    private def copyFromSlottedRow(other: SlottedRow,
                                   otherLongOffset: Int,
                                   otherRefOffset: Int,
                                   thisLongOffset: Int,
                                   thisRefOffset: Int,
                                   nLongs: Int,
                                   nRefs: Int): Unit = {
      if (nLongs > longsPerRow || nRefs > refsPerRow) {
        throw new InternalException("A bug has occurred in the morsel runtime: The target morsel cannot hold the data to copy.")
      } else {
        System.arraycopy(other.longs, otherLongOffset, longs, longOffset(thisLongOffset), nLongs)
        System.arraycopy(other.refs, otherRefOffset, refs, refOffset(thisRefOffset), nRefs)
      }
    }

    private def copyFromMorselRow(other: MorselDerivedRow,
                                  otherLongOffset: Int,
                                  otherRefOffset: Int,
                                  thisLongOffset: Int,
                                  thisRefOffset: Int,
                                  nLongs: Int,
                                  nRefs: Int): Unit = {
      if (nLongs > longsPerRow || nRefs > refsPerRow) {
        throw new InternalException("A bug has occurred in the morsel runtime: The target morsel cannot hold the data to copy.")
      } else {
        val m = other.morsel
        System.arraycopy(m.longs, other.longOffset(otherLongOffset), longs, longOffset(thisLongOffset), nLongs)
        System.arraycopy(m.refs, other.refOffset(otherRefOffset), refs, refOffset(thisRefOffset), nRefs)
      }
    }

    override def morsel: Morsel = morselSelf

    override def copyAllToSlottedRow(target: SlottedRow): Unit = copyToSlottedRow(target, longsPerRow, refsPerRow)

    override def copyToSlottedRow(target: SlottedRow, nLongs: Int, nRefs: Int): Unit = {
      System.arraycopy(longs, longOffset(0), target.longs, 0, nLongs)
      System.arraycopy(refs, refOffset(0), target.refs, 0, nRefs)
    }

    override def longOffset(offsetInRow: Int): Int = currentRow * longsPerRow + offsetInRow

    override def refOffset(offsetInRow: Int): Int = currentRow * refsPerRow + offsetInRow

    override def toString: String = {
      val sb = new mutable.StringBuilder()
      sb ++= getClass.getSimpleName
      sb ++= "[row: "
      sb.append(row)
      sb ++= ", "
      if (onValidRow()) {
        val longStrings = longs.slice(row * longsPerRow, (row+1)*longsPerRow).map(String.valueOf)
        val refStrings = refs.slice(row * refsPerRow, (row+1)*refsPerRow).map(String.valueOf)
        for (str <- longStrings) {
          sb ++= str
          sb += ' '
        }
        sb += ' '
        sb += ' '
        for (str <- refStrings) {
          sb ++= str
          sb += ' '
        }
        addPrettyRowMarker(sb, row)
        sb += ']'
      } else {
        sb ++= "<invalid_row>]"
      }
      sb.result()
    }

    override def shallowInstanceHeapUsage: Long = Cursor.SHALLOW_SIZE

    override type Position = Int

    override def position: Position = row

    override def setPosition(position: Position): Unit = setRow(position)
  }

  object Cursor {
    final val SHALLOW_SIZE = shallowSizeOfInstance(classOf[Cursor])
  }

  // ====================

  def shallowCopy(): Morsel = new Morsel(longs, refs, slots, maxNumberOfRows, startRow, endRow)

  def filteringShallowCopy(): FilteringMorsel = new FilteringMorsel(longs, refs, slots, maxNumberOfRows, startRow, endRow)

  /**
    * Adapt the valid rows of the morsel so that the last valid row is the previous one according to the current position.
    * This usually happens after one operator finishes writing to a morsel.
    */
  def truncateToRow(row: Int): Unit = {
    endRow = row
  }

  @inline def numberOfRows: Int = endRow - startRow

  @inline def hasData: Boolean = numberOfRows > 0

  @inline def isEmpty: Boolean = !hasData

  /**
   * @param start first index of the view (inclusive start)
   * @param end first index after the view (exclusive end)
   * @return a shallow copy that is configured to only see the configured view.
   */
  def view(start: Int, end: Int): Morsel = {
    val view = shallowCopy()
    view.startRow = start
    view.endRow = end
    view
  }

  /**
   * Copies from input to the beginning of this morsel. Input is assumed not to contain any cancelledRows
   */
  def compactRowsFrom(input: Morsel): Unit = {
    checkOnlyWhenAssertionsAreEnabled(!input.isInstanceOf[FilteringMorsel] && numberOfRows >= input.numberOfRows)

    if (longsPerRow > 0) {
      System.arraycopy(input.longs,
        input.startRow * input.longsPerRow,
        longs,
        startRow * longsPerRow,
        input.numberOfRows * longsPerRow)
    }
    if (refsPerRow > 0) {
      System.arraycopy(input.refs,
        input.startRow * input.refsPerRow,
        refs,
        startRow * refsPerRow,
        input.numberOfRows * refsPerRow)
    }
  }

  override def toString: String = {
    s"Morsel[0x${System.identityHashCode(this).toHexString}](longsPerRow=$longsPerRow, refsPerRow=$refsPerRow, maxRows=$maxNumberOfRows, startRow=$startRow endRow=$endRow)"
  }

  def prettyString(currentRow: Int): Seq[String] = {
    val longStrings = longs.slice(startRow*longsPerRow, endRow*longsPerRow).map(String.valueOf)
    val refStrings = refs.slice(startRow*refsPerRow, endRow*refsPerRow).map(String.valueOf)

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
    for (row <- 0 until (endRow-startRow)) {
      sb ++= (if ((startRow + row) == currentRow) " * " else "   ")

      for (col <- 0 until longsPerRow) {
        val width = longWidths(col)
        if (width > 0)
          sb ++= ("%" + width + "s").format(longStrings(row * longsPerRow + col))
        sb += ' '
      }
      sb += ' '
      sb += ' '
      for (col <- 0 until refsPerRow) {
        val width = refWidths(col)
        if (width > 0)
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
    * Total heap usage of all valid rows (can be a view, so might not be the whole morsel).
    * The reasoning behind this is that the other parts of the morsel would be part of other views in other buffers/argument states and will
    * also be accounted for.
    *
    * TODO: When we have morsel reuse we can track the actual memory usage of the morsel data more correctly and just add the overhead of each view separately
    *       (and then we can use the more accurate HeapEstimator.sizeOf(longs) + HeapEstimator.shallowSizeOf(refs.asInstanceOf[Array[Object]]) etc.)
    */
  override def estimatedHeapUsage: Long = {
    Morsel.INSTANCE_SIZE + estimatedHeapUsageOfView
  }

  protected def estimatedHeapUsageOfView: Long = {
    val nRows = numberOfRows
    var usage = longsPerRow * nRows * 8L
    if (refsPerRow > 0) {
      usage += refsPerRow * nRows * org.neo4j.memory.HeapEstimator.OBJECT_REFERENCE_BYTES
      var i = startRow * refsPerRow
      val limit = endRow * refsPerRow
      while (i < limit) {
        val ref = refs(i)
        if (ref != null) {
          usage += ref.estimatedHeapUsage()
        }
        i += 1
      }
    }
    usage
  }

  private def fail(): Nothing =
    throw new InternalException("Tried using a wrong row.")
}

trait MorselIndexedSeq {
  def morsels: IndexedSeq[Morsel]

  /**
   * Returns a [[MorselReadCursor]] over all containing morsels.
   */
  def readCursor(onFirstRow: Boolean = false): MorselReadCursor = {
    if (morsels.nonEmpty) new Cursor(onFirstRow)
    else Morsel.empty.readCursor(onFirstRow)
  }

  class Cursor(onFirstRow: Boolean) extends MorselReadCursor {
    require(morsels.nonEmpty)

    private var morselCursor: MorselReadCursor = morsels(0).readCursor(onFirstRow = false)
    private var nextMorselSeqIndex = 1

    if (onFirstRow) next()

    @inline
    private[this] def setMorselCursor(index: Int) = {
      morselCursor = morsels(index).readCursor()
      nextMorselSeqIndex = index + 1
    }

    override def getLongAt(offset: Int): Long = morselCursor.getLongAt(offset)
    override def getRefAt(offset: Int): AnyValue = morselCursor.getRefAt(offset)
    override def getByName(name: String): AnyValue = morselCursor.getByName(name)
    override def getLinenumber: Option[ResourceLinenumber] = morselCursor.getLinenumber
    override def setRow(row: Int): Unit = morselCursor.setRow(row)

    override def setToStart(): Unit = setMorselCursor(0)

    override def setToEnd(): Unit = {
      setMorselCursor(morsels.size - 1)
      morselCursor.setToEnd()
    }

    override def onValidRow(): Boolean = morselCursor != null && morselCursor.onValidRow()

    @tailrec
    override final def next(): Boolean = {
      if (morselCursor.next()) {
        true
      } else if (nextMorselSeqIndex >= morsels.size) {
        false
      } else {
        morselCursor = morsels(nextMorselSeqIndex).readCursor()
        nextMorselSeqIndex += 1
        next()
      }
    }

    override def hasNext: Boolean = morselCursor.hasNext || upcomingMorselsHasNext

    private def upcomingMorselsHasNext(): Boolean = {
      var i = nextMorselSeqIndex
      val size = morsels.size
      while (i < size) {
        if (morsels(i).hasData) return true
        i += 1
      }
      false
    }

    override def snapshot(): MorselRow = morselCursor.snapshot()

    override def row: Int = throw new UnsupportedOperationException("row not supported in MorselData.Cursor")

    override def morsel: Morsel = morselCursor.morsel

    override def longOffset(offsetInRow: Int): Int = morselCursor.longOffset(offsetInRow)

    override def refOffset(offsetInRow: Int): Int = morselCursor.refOffset(offsetInRow)

    override def shallowInstanceHeapUsage: Long = SHALLOW_SIZE

    override def getCachedProperty(key: ASTCachedProperty): Value = morselCursor.getCachedProperty(key)

    override def getCachedPropertyAt(offset: Int): Value = morselCursor.getCachedPropertyAt(offset)

    override def setCachedProperty(key: ASTCachedProperty, value: Value): Unit = morselCursor.setCachedProperty(key, value)

    override def setCachedPropertyAt(offset: Int, value: Value): Unit = morselCursor.setCachedPropertyAt(offset, value)

    override def copyAllToSlottedRow(target: SlottedRow): Unit = morselCursor.copyAllToSlottedRow(target)

    override def copyToSlottedRow(target: SlottedRow, nLongs: Int, nRefs: Int): Unit = morselCursor.copyToSlottedRow(target, nLongs, nRefs)

    override type Position = (Int, Int)

    override def position: Position = (nextMorselSeqIndex - 1, morselCursor.row)

    override def setPosition(position: Position): Unit = {
      val morselSeqIndex = position._1
      if (morselSeqIndex != nextMorselSeqIndex - 1) {
        nextMorselSeqIndex = morselSeqIndex + 1
        morselCursor = morsels(morselSeqIndex).readCursor()
      }
      morselCursor.setRow(position._2)
    }
  }

  object Cursor {
    final val SHALLOW_SIZE = shallowSizeOfInstance(classOf[Cursor])
  }
}
