/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.helpers

object NullChecker {
  val NULL_ENTITY: Long = -1L
  def entityIsNull(entityId: Long): Boolean = entityId == NULL_ENTITY
}
