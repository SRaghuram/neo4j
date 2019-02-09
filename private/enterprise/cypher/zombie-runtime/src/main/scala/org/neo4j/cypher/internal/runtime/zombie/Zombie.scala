/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

object Zombie {
  val DEBUG = false
  def debug(msg: => String): Unit =
    if (DEBUG)
      print(msg + "\n")
}
