/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking;

import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.storageengine.api.ConstraintRule;

import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;

public class SchemaRuleUtil
{
    private SchemaRuleUtil()
    {
    }

    public static ConstraintRule uniquenessConstraintRule( long ruleId, int labelId, int propertyId, long indexId )
    {
        return ConstraintRule.constraintRule( ruleId,
                ConstraintDescriptorFactory.uniqueForLabel( labelId, propertyId ), indexId );
    }

    public static ConstraintRule nodePropertyExistenceConstraintRule( long ruleId, int labelId, int propertyId )
    {
        return ConstraintRule.constraintRule( ruleId,
                ConstraintDescriptorFactory.existsForLabel( labelId, propertyId ) );
    }

    public static ConstraintRule relPropertyExistenceConstraintRule( long ruleId, int relTypeId, int propertyId )
    {
        return ConstraintRule.constraintRule( ruleId,
                ConstraintDescriptorFactory.existsForRelType( relTypeId, propertyId ) );
    }

    public static IndexDescriptor indexRule( long ruleId, int labelId, int propertyId, IndexProviderDescriptor descriptor )
    {
        return IndexPrototype.forSchema( forLabel( labelId, propertyId ), descriptor ).materialise( ruleId );
    }

    public static IndexDescriptor constraintIndexRule( long ruleId, int labelId, int propertyId,
                                                            IndexProviderDescriptor descriptor, long constraintId )
    {
        return IndexPrototype.uniqueForSchema( forLabel( labelId, propertyId ), descriptor ).materialise( ruleId ).withOwningConstraintId( constraintId );
    }

    public static IndexDescriptor constraintIndexRule( long ruleId, int labelId, int propertyId,
                                                            IndexProviderDescriptor descriptor )
    {
        return IndexPrototype.uniqueForSchema( forLabel( labelId, propertyId ), descriptor ).materialise( ruleId );
    }
}
