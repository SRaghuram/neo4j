/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commercial.edition.factory;

import com.neo4j.commercial.edition.CommercialEditionModule;

import java.io.File;

import org.neo4j.common.Edition;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

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
        public DatabaseManagementService newDatabase( Config config )
        {
            File absoluteStoreDir = storeDir.getAbsoluteFile();
            File databasesRoot;
            if ( config.isConfigured( GraphDatabaseSettings.databases_root_path ) )
            {
                databasesRoot = config.get( GraphDatabaseSettings.databases_root_path );
            }
            else
            {
                databasesRoot = absoluteStoreDir.getParentFile();
                if ( !config.isConfigured( GraphDatabaseSettings.default_database ) )
                {
                    config.augment( GraphDatabaseSettings.default_database, absoluteStoreDir.getName() );
                }
            }
            config.augment( GraphDatabaseSettings.databases_root_path, databasesRoot.getAbsolutePath() );
            return new GraphDatabaseFacadeFactory( DatabaseInfo.COMMERCIAL, CommercialEditionModule::new )
                    .initFacade( storeDir, config, state.databaseDependencies(), new GraphDatabaseFacade() );
        }
    }
}
