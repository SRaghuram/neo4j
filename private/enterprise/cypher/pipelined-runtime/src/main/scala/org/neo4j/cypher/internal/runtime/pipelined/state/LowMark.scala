/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import java.util.concurrent.atomic.AtomicInteger

/**
 * A LowMark is used to keep track of the lowest observed value.
 */
trait LowMark {
  /**
   * Return the current lowest observed value.
   */
  def get(): Int

  /**
   * Report a new observed value, which updates the mark if `x` is lower then the current mark.
   */
  def lower(x: Int): Unit
}

class StandardLowMark(startValue: Int) extends LowMark {
  private var mark = startValue

  override def get(): Int = mark
  override def lower(x: Int): Unit = mark = math.min(x, mark)
}

class ConcurrentLowMark(startValue: Int) extends LowMark {
  private val mark = new AtomicInteger(startValue)

  override def get(): Int = mark.get
  override def lower(x: Int): Unit = mark.updateAndGet(current => math.min(current, x))
}
