/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.helpers

object NullChecker {
  final val NULL_ENTITY: Long = -1L
  final def entityIsNull(entityId: Long): Boolean = entityId == NULL_ENTITY
}
