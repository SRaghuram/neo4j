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
package org.neo4j.graphdb;

import org.junit.Test;

import org.neo4j.graphdb.schema.IndexDefinition;

import static org.neo4j.graphdb.IndexDefinitionFacadeMethods.ALL_INDEX_DEFINITION_FACADE_METHODS;

public class MandatoryTransactionsForIndexDefinitionTest
    extends AbstractMandatoryTransactionsTest<IndexDefinition>
{
    @Test
    public void shouldRequireTransactionsWhenCallingMethodsOnIndexDefinitions()
    {
        assertFacadeMethodsThrowNotInTransaction( obtainEntity(), ALL_INDEX_DEFINITION_FACADE_METHODS );
    }

    @Test
    public void shouldTerminateWhenCallingMethodsOnIndexDefinitions()
    {
        assertFacadeMethodsThrowAfterTerminate( ALL_INDEX_DEFINITION_FACADE_METHODS );
    }

    @Override
    protected IndexDefinition obtainEntityInTransaction( GraphDatabaseService graphDatabaseService )
    {
        return graphDatabaseService
               .schema()
               .indexFor( Label.label( "Label" ) )
               .on( "property" )
               .create();
    }
}
