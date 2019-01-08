/*
 * Copyright (c) 2002-2019 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.util.dbstructure;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;

public class EnterpriseDbStructureTool extends DbStructureTool
{
    private EnterpriseDbStructureTool()
    {
    }

    public static void main( String[] args ) throws IOException
    {
        new EnterpriseDbStructureTool().run( args );
    }

    @Override
    protected GraphDatabaseService instantiateGraphDatabase( String dbDir )
    {
        return new EnterpriseGraphDatabaseFactory().newEmbeddedDatabase( new File( dbDir ) );
    }
}
