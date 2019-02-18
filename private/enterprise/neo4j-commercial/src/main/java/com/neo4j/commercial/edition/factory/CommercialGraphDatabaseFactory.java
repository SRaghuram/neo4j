/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commercial.edition.factory;

import com.neo4j.commercial.edition.CommercialGraphDatabase;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.Edition;

public class CommercialGraphDatabaseFactory extends GraphDatabaseFactory
{
    @Override
    protected GraphDatabaseBuilder.DatabaseCreator createDatabaseCreator( File storeDir, GraphDatabaseFactoryState state )
    {
        return new CommercialDatabaseCreator( storeDir, state );
    }

    @Override
    public String getEdition()
    {
        return Edition.COMMERCIAL.toString();
    }

    private static class CommercialDatabaseCreator implements GraphDatabaseBuilder.DatabaseCreator
    {
        private final File storeDir;
        private final GraphDatabaseFactoryState state;

        CommercialDatabaseCreator( File storeDir, GraphDatabaseFactoryState state )
        {
            this.storeDir = storeDir;
            this.state = state;
        }

        @Override
        public GraphDatabaseService newDatabase( Config config )
        {
            File absoluteStoreDir = storeDir.getAbsoluteFile();
            File databasesRoot = absoluteStoreDir.getParentFile();
            config.augment( GraphDatabaseSettings.active_database, absoluteStoreDir.getName() );
            config.augment( GraphDatabaseSettings.databases_root_path, databasesRoot.getAbsolutePath() );
            return new CommercialGraphDatabase( databasesRoot, config, state.databaseDependencies() );
        }
    }
}
