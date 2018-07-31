/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.graphdb;

import org.neo4j.graphdb.schema.IndexDefinition;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;

public class IndexDefinitionFacadeMethods
{
    private static final FacadeMethod<IndexDefinition> GET_LABEL = new FacadeMethod<>( "Label getLabel()", IndexDefinition::getLabel );
    private static final FacadeMethod<IndexDefinition> GET_PROPERTY_KEYS =
            new FacadeMethod<>( "Iterable<String> getPropertyKeys()", IndexDefinition::getPropertyKeys );
    private static final FacadeMethod<IndexDefinition> DROP = new FacadeMethod<>( "void drop()", IndexDefinition::drop );
    private static final FacadeMethod<IndexDefinition> IS_CONSTRAINT_INDEX =
            new FacadeMethod<>( "boolean isConstraintIndex()", IndexDefinition::isConstraintIndex );

    static final Iterable<FacadeMethod<IndexDefinition>> ALL_INDEX_DEFINITION_FACADE_METHODS =
            unmodifiableCollection( asList( GET_LABEL, GET_PROPERTY_KEYS, DROP, IS_CONSTRAINT_INDEX ) );

    private IndexDefinitionFacadeMethods()
    {
    }
}
