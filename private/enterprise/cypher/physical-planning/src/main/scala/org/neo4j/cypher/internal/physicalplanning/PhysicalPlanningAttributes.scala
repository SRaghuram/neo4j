/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.Size
import org.neo4j.cypher.internal.v4_0.util.attribution.{Attribute, Id}

object PhysicalPlanningAttributes {
  class SlotConfigurations extends Attribute[LogicalPlan, SlotConfiguration]
  class ArgumentSizes extends Attribute[LogicalPlan, Size]
  class ApplyPlans extends Attribute[LogicalPlan, Id]
  class NestedPlanArgumentConfigurations extends Attribute[LogicalPlan, SlotConfiguration]
}
