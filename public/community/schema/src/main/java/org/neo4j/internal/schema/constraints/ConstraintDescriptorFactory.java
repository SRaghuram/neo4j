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
package org.neo4j.internal.schema.constraints;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.IndexDescriptor2;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaComputer;
import org.neo4j.internal.schema.SchemaDescriptor;

import static java.lang.String.format;

public class ConstraintDescriptorFactory
{
    private ConstraintDescriptorFactory()
    {
    }

    public static NodeExistenceConstraintDescriptor existsForLabel( int labelId, int... propertyIds )
    {
        return new NodeExistenceConstraintDescriptor( SchemaDescriptor.forLabel( labelId, propertyIds ) );
    }

    public static RelExistenceConstraintDescriptor existsForRelType( int relTypeId, int... propertyIds )
    {
        return new RelExistenceConstraintDescriptor( SchemaDescriptor.forRelType( relTypeId, propertyIds ) );
    }

    public static UniquenessConstraintDescriptor uniqueForLabel( int labelId, int... propertyIds )
    {
        return uniqueForSchema( SchemaDescriptor.forLabel( labelId, propertyIds ) );
    }

    public static NodeKeyConstraintDescriptor nodeKeyForLabel( int labelId, int... propertyIds )
    {
        return nodeKeyForSchema( SchemaDescriptor.forLabel( labelId, propertyIds ) );
    }

    public static AbstractConstraintDescriptor existsForSchema( SchemaDescriptor schema )
    {
        return schema.computeWith( convertToExistenceConstraint );
    }

    public static NodeExistenceConstraintDescriptor existsForSchema( LabelSchemaDescriptor schema )
    {
        return new NodeExistenceConstraintDescriptor( schema );
    }

    public static RelExistenceConstraintDescriptor existsForSchema( RelationTypeSchemaDescriptor schema )
    {
        return new RelExistenceConstraintDescriptor( schema );
    }

    public static UniquenessConstraintDescriptor uniqueForSchema( SchemaDescriptor schema )
    {
        return schema.computeWith( convertToUniquenessConstraint );
    }

    public static NodeKeyConstraintDescriptor nodeKeyForSchema( SchemaDescriptor schema )
    {
        return schema.computeWith( convertToNodeKeyConstraint );
    }

    private static SchemaComputer<AbstractConstraintDescriptor> convertToExistenceConstraint = new SchemaComputer<>()
    {
        @Override
        public AbstractConstraintDescriptor computeSpecific( LabelSchemaDescriptor schema )
        {
            return new NodeExistenceConstraintDescriptor( schema );
        }

        @Override
        public AbstractConstraintDescriptor computeSpecific( RelationTypeSchemaDescriptor schema )
        {
            return new RelExistenceConstraintDescriptor( schema );
        }

        @Override
        public AbstractConstraintDescriptor computeSpecific( SchemaDescriptor schema )
        {
            throw new UnsupportedOperationException( format( "Cannot create existence constraint for schema '%s' of type %s",
                    schema.userDescription( TokenNameLookup.idTokenNameLookup ), schema.getClass().getSimpleName() ) );
        }
    };

    private static SchemaComputer<UniquenessConstraintDescriptor> convertToUniquenessConstraint = new SchemaComputer<>()
    {
        @Override
        public UniquenessConstraintDescriptor computeSpecific( LabelSchemaDescriptor schema )
        {
            return new UniquenessConstraintDescriptor( schema );
        }

        @Override
        public UniquenessConstraintDescriptor computeSpecific( RelationTypeSchemaDescriptor schema )
        {
            throw new UnsupportedOperationException( format( "Cannot create uniqueness constraint for schema '%s' of type %s",
                    schema.userDescription( TokenNameLookup.idTokenNameLookup ), schema.getClass().getSimpleName() ) );
        }

        @Override
        public UniquenessConstraintDescriptor computeSpecific( SchemaDescriptor schema )
        {
            throw new UnsupportedOperationException( format( "Cannot create uniqueness constraint for schema '%s' of type %s",
                    schema.userDescription( TokenNameLookup.idTokenNameLookup ), schema.getClass().getSimpleName() ) );
        }
    };

    private static SchemaComputer<NodeKeyConstraintDescriptor> convertToNodeKeyConstraint = new SchemaComputer<>()
    {
        @Override
        public NodeKeyConstraintDescriptor computeSpecific( LabelSchemaDescriptor schema )
        {
            return new NodeKeyConstraintDescriptor( schema );
        }

        @Override
        public NodeKeyConstraintDescriptor computeSpecific( RelationTypeSchemaDescriptor schema )
        {
            throw new UnsupportedOperationException( format( "Cannot create node key constraint for schema '%s' of type %s",
                    schema.userDescription( TokenNameLookup.idTokenNameLookup ), schema.getClass().getSimpleName() ) );
        }

        @Override
        public NodeKeyConstraintDescriptor computeSpecific( SchemaDescriptor schema )
        {
            throw new UnsupportedOperationException( format( "Cannot create node key constraint for schema '%s' of type %s",
                    schema.userDescription( TokenNameLookup.idTokenNameLookup ), schema.getClass().getSimpleName() ) );
        }
    };
}
