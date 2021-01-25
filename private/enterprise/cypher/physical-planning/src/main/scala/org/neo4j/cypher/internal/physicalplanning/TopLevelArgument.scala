/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

/**
  * The top level argument is the argument at the top level of a logical plan.
  * Here, there is no outer apply, meaning that only a single execution will happen,
  * started by a single argument row. This gives us opportunity to specialize handling
  * of arguments at the top level, and optimize storing of the logical argument.
  */
object TopLevelArgument {
  class TopLevelArgumentException(argument: Long) extends RuntimeException(
    "The top level argument has to be 0, but got " + argument)

  def assertTopLevelArgument(argument: Long): Unit = {
    if (argument != VALUE) {
      throw new TopLevelArgumentException(argument)
    }
  }

  val VALUE: Long = 0
  val SLOT_OFFSET: Int = -1
}
