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
package org.neo4j.graphdb.factory;

import java.io.File;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.enterprise.EnterpriseGraphDatabase;
import org.neo4j.kernel.impl.factory.Edition;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;

import static org.neo4j.kernel.configuration.Settings.FALSE;

/**
 * Factory for Neo4j database instances with Enterprise Edition features.
 *
 * @see org.neo4j.graphdb.factory.GraphDatabaseFactory
 */
public class EnterpriseGraphDatabaseFactory extends GraphDatabaseFactory
{
    @Override
    protected GraphDatabaseBuilder.DatabaseCreator createDatabaseCreator( final File storeDir,
                                                                          final GraphDatabaseFactoryState state )
    {
        return new GraphDatabaseBuilder.DatabaseCreator()
        {
            @Override
            public GraphDatabaseService newDatabase( Map<String,String> config )
            {
                return newDatabase( Config.defaults( config ) );
            }

            @Override
            public GraphDatabaseService newDatabase( Config config )
            {
                config.augment( GraphDatabaseFacadeFactory.Configuration.ephemeral, FALSE );
                return new EnterpriseGraphDatabase( storeDir,
                        config,
                        state.databaseDependencies() );
            }
        };
    }

    @Override
    public String getEdition()
    {
        return Edition.enterprise.toString();
    }
}
