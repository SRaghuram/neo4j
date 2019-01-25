/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physical_planning

import org.neo4j.cypher.internal.physical_planning.SlotConfiguration.Size
import org.neo4j.cypher.internal.v4_0.util.attribution.Attribute

object PhysicalPlanningAttributes {
  class SlotConfigurations extends Attribute[SlotConfiguration]
  class ArgumentSizes extends Attribute[Size]
}
