/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, ResourceLinenumber}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.v4_0.logical.plans.CachedNodeProperty
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{Value, Values}
import org.neo4j.cypher.internal.v4_0.util.InternalException

object MorselExecutionContext {
  def apply(morsel: Morsel, pipeline: Pipeline) = new MorselExecutionContext(morsel,
    pipeline.slots.numberOfLongs, pipeline.slots.numberOfReferences, 0)
  def apply(morsel: Morsel, numberOfLongs: Int, numberOfRows: Int) = new MorselExecutionContext(morsel,
    numberOfLongs, numberOfRows, 0)
  val EMPTY = new MorselExecutionContext(Morsel.create(SlotConfiguration.empty, 1), 0, 0, 0)

  def createSingleRow(): MorselExecutionContext =
    EMPTY.createClone()
}

class MorselExecutionContext(private val morsel: Morsel, private val longsPerRow: Int, private val refsPerRow: Int, private var currentRow: Int) extends ExecutionContext {

  def moveToNextRow(): Unit = {
    currentRow += 1
  }

  def getCurrentRow: Int = currentRow

  def getLongsPerRow: Int = longsPerRow

  def getRefsPerRow: Int = refsPerRow

  def moveToRow(row: Int): Unit = currentRow = row

  def resetToFirstRow(): Unit = currentRow = 0

  /**
    * Checks if the morsel has more rows
    */
  def hasMoreRows: Boolean = currentRow < morsel.validRows

  def numberOfRows: Int = morsel.validRows

  /**
    * Check so that there is at least one valid row of data
    */
  def hasData: Boolean = morsel.validRows > 0

  /**
    * Set the valid rows of the morsel to the current position, which usually
    * happens after one operator finishes writing to a morsel.
    */
  def finishedWriting(): Unit = morsel.validRows = currentRow

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
        throw new InternalException("Tried to copy too much data.")
      else {
        System.arraycopy(other.morsel.longs, other.longsAtCurrentRow, morsel.longs, longsAtCurrentRow, nLongs)
        System.arraycopy(other.morsel.refs, other.refsAtCurrentRow, morsel.refs, refsAtCurrentRow, nRefs)
      }

    case other:SlottedExecutionContext =>
      if (nLongs > longsPerRow || nRefs > refsPerRow)
        throw new InternalException("Tried to copy too much data.")
      else {
        System.arraycopy(other.longs, 0, morsel.longs, longsAtCurrentRow, nLongs)
        System.arraycopy(other.refs, 0, morsel.refs, refsAtCurrentRow, nRefs)
      }
    case _ => fail()
  }

  override def copyCachedFrom(input: ExecutionContext): Unit = ???

  override def toString(): String = {
    s"MorselExecutionContext[0x${System.identityHashCode(this).toHexString}](morsel[0x${System.identityHashCode(morsel).toHexString}]=$morsel, longsPerRow=$longsPerRow, refsPerRow=$refsPerRow, currentRow=$currentRow)"
  }

  /**
    * Copies the whole row from input to this.
    * @param input
    */
  def copyFrom(input: MorselExecutionContext): Unit = copyFrom(input, input.longsPerRow, input.refsPerRow)

  override def setLongAt(offset: Int, value: Long): Unit = morsel.longs(currentRow * longsPerRow + offset) = value

  override def getLongAt(offset: Int): Long = morsel.longs(currentRow * longsPerRow + offset)

  override def setRefAt(offset: Int, value: AnyValue): Unit = morsel.refs(currentRow * refsPerRow + offset) = value

  override def getRefAt(offset: Int): AnyValue = morsel.refs(currentRow * refsPerRow + offset)

  override def set(newEntries: Seq[(String, AnyValue)]): Unit = fail()

  override def set(key1: String, value1: AnyValue): Unit = fail()

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue): Unit = fail()

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): Unit = fail()

  override def mergeWith(other: ExecutionContext): Unit = fail()

  override def createClone(): MorselExecutionContext = new MorselExecutionContext(morsel, longsPerRow, refsPerRow, currentRow)

  override def +=(kv: (String, AnyValue)): MorselExecutionContext.this.type = fail()

  override def -=(key: String): MorselExecutionContext.this.type = fail()

  override def get(key: String): Option[AnyValue] = fail()

  override def iterator: Iterator[(String, AnyValue)] = {
    // This method implementation is for debug usage only (the debugger will invoke it when stepping).
    // Please do not use in production code.
    val longRow =  morsel.longs.slice(currentRow * longsPerRow, (currentRow + 1) * longsPerRow)
    val refRow =  morsel.refs.slice(currentRow * refsPerRow, (currentRow + 1) * refsPerRow)
    Iterator.single(s"${this.toString()}" -> Values.stringValue(s"""${longRow.mkString("[", ",", "]")}${refRow.mkString("[", ",", "]")}"""))
  }

  override def copyWith(key1: String, value1: AnyValue) = fail()

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue) = fail()

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): ExecutionContext = fail()

  override def copyWith(newEntries: Seq[(String, AnyValue)]): ExecutionContext = fail()

  override def boundEntities(materializeNode: Long => AnyValue, materializeRelationship: Long => AnyValue): Map[String, AnyValue] = fail()

  override def isNull(key: String): Boolean = fail()

  override def setCachedProperty(key: CachedNodeProperty, value: Value): Unit = fail()

  override def setCachedPropertyAt(offset: Int, value: Value): Unit = setRefAt(offset, value)

  override def getCachedProperty(key: CachedNodeProperty): Value = fail()

  override def getCachedPropertyAt(offset: Int): Value = getRefAt(offset).asInstanceOf[Value]

  private def longsAtCurrentRow: Int = currentRow * longsPerRow

  private def refsAtCurrentRow: Int = currentRow * refsPerRow

  private def fail(): Nothing =
    throw new InternalException("Tried using a wrong context.")

  override def setLinenumber(file: String, line: Long, last: Boolean = false): Unit = ???

  override def setLinenumber(line: Option[ResourceLinenumber]): Unit = ???

  override def getLinenumber: Option[ResourceLinenumber] = ???
}
