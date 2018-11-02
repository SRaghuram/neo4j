/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.graphdb.factory;

import java.io.File;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.factory.Edition;

import static java.util.Arrays.asList;

/**
 * Factory for Neo4j database instances with Enterprise Edition and High-Availability features.
 *
 * @see org.neo4j.graphdb.factory.GraphDatabaseFactory
 * @deprecated high availability database/edition is deprecated in favour of causal clustering. It will be removed in next major release.
 */
@Deprecated
public class HighlyAvailableGraphDatabaseFactory extends GraphDatabaseFactory
{
    public HighlyAvailableGraphDatabaseFactory()
    {
        super( highlyAvailableFactoryState() );
    }

    private static GraphDatabaseFactoryState highlyAvailableFactoryState()
    {
        GraphDatabaseFactoryState state = new GraphDatabaseFactoryState();
        state.addSettingsClasses( asList( ClusterSettings.class, HaSettings.class ) );
        return state;
    }

    @Override
    protected GraphDatabaseBuilder.DatabaseCreator createDatabaseCreator(
            final File storeDir, final GraphDatabaseFactoryState state )
    {
        return new GraphDatabaseBuilder.DatabaseCreator()
        {
            @Override
            public GraphDatabaseService newDatabase( Config config )
            {
                File absoluteStoreDir = storeDir.getAbsoluteFile();
                File databasesRoot = absoluteStoreDir.getParentFile();
                config.augment( GraphDatabaseSettings.ephemeral, Settings.FALSE );
                config.augment( GraphDatabaseSettings.active_database, absoluteStoreDir.getName() );
                config.augment( GraphDatabaseSettings.databases_root_path, databasesRoot.getAbsolutePath() );
                return new HighlyAvailableGraphDatabase( databasesRoot, config, state.databaseDependencies() );
            }
        };
    }

    @Override
    public String getEdition()
    {
        return Edition.enterprise.toString();
    }
}
