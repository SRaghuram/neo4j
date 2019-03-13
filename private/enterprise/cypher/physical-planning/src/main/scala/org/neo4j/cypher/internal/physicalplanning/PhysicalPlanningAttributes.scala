/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.Size
import org.neo4j.cypher.internal.v4_0.util.attribution.{Attribute, Id}

object PhysicalPlanningAttributes {
  class SlotConfigurations extends Attribute[SlotConfiguration]
  class ArgumentSizes extends Attribute[Size]
  class ApplyPlans extends Attribute[Id]
  class NestedPlanArgumentConfigurations extends Attribute[SlotConfiguration]
}
