/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test;

import com.neo4j.commercial.edition.CommercialEditionModule;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.io.File;

import org.neo4j.common.Edition;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.Settings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactoryState;

import static org.neo4j.configuration.Settings.FALSE;

public class TestCommercialGraphDatabaseFactory extends TestGraphDatabaseFactory
{
    public TestCommercialGraphDatabaseFactory()
    {
        super();
    }

    public TestCommercialGraphDatabaseFactory( LogProvider logProvider )
    {
        super( logProvider );
    }

    @Override
    protected GraphDatabaseBuilder.DatabaseCreator createDatabaseCreator( File storeDir,
            GraphDatabaseFactoryState state )
    {
        return new GraphDatabaseBuilder.DatabaseCreator()
        {
            @Override
            public GraphDatabaseService newDatabase( Config config )
            {
                File databasesRoot = storeDir.getParentFile();
                augmentConfig( config, databasesRoot, storeDir );
                TestGraphDatabaseFactoryState testState = (TestGraphDatabaseFactoryState) state;
                TestCommercialGraphDatabaseFacadeFactory facadeFactory = new TestCommercialGraphDatabaseFacadeFactory( testState, false );
                return facadeFactory.newFacade( databasesRoot, config, GraphDatabaseDependencies.newDependencies( state.databaseDependencies() ) );
            }
        };
    }

    private static void augmentConfig( Config config, File databasesRoot, File storeDir )
    {
        config.augment( GraphDatabaseSettings.ephemeral, FALSE );
        config.augment( GraphDatabaseSettings.active_database, storeDir.getName() );
        config.augment( GraphDatabaseSettings.databases_root_path, databasesRoot.getAbsolutePath() );
        config.augment( OnlineBackupSettings.online_backup_listen_address, "127.0.0.1:0" );
        if ( !config.isConfigured( OnlineBackupSettings.online_backup_enabled ) )
        {
            config.augment( OnlineBackupSettings.online_backup_enabled, FALSE );
        }
    }

    @Override
    protected GraphDatabaseBuilder.DatabaseCreator createImpermanentDatabaseCreator( final File storeDir,
            final TestGraphDatabaseFactoryState state )
    {
        return config ->
        {
            augmentConfig( config, storeDir.getParentFile(), storeDir );
            return new TestCommercialGraphDatabaseFacadeFactory( state, true ).newFacade( storeDir, config,
                    GraphDatabaseDependencies.newDependencies( state.databaseDependencies() ) );
        };
    }

    @Override
    public String getEdition()
    {
        return Edition.COMMERCIAL.toString();
    }

    private static class TestCommercialGraphDatabaseFacadeFactory extends TestGraphDatabaseFacadeFactory
    {
        TestCommercialGraphDatabaseFacadeFactory( TestGraphDatabaseFactoryState state, boolean impermanent )
        {
            super( state, impermanent, DatabaseInfo.COMMERCIAL, CommercialEditionModule::new );
        }

        @Override
        protected File configureAndGetDatabaseRoot( File storeDir, Config config )
        {
            return storeDir;
        }
    }
}

