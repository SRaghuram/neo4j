/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test;

import com.neo4j.kernel.impl.enterprise.EnterpriseEditionModule;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.Edition;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactoryState;

/**
 * Factory for test graph database.
 */
//TODO:remove
public class TestEnterpriseGraphDatabaseFactory extends TestGraphDatabaseFactory
{
    public TestEnterpriseGraphDatabaseFactory()
    {
        super();
    }

    public TestEnterpriseGraphDatabaseFactory( LogProvider logProvider )
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
                File absoluteStoreDir = storeDir.getAbsoluteFile();
                File databasesRoot = absoluteStoreDir.getParentFile();
                if ( !config.isConfigured( GraphDatabaseSettings.shutdown_transaction_end_timeout ) )
                {
                    config.augment( GraphDatabaseSettings.shutdown_transaction_end_timeout, "0s" );
                }
                config.augment( GraphDatabaseSettings.ephemeral, Settings.FALSE );
                config.augment( GraphDatabaseSettings.active_database, absoluteStoreDir.getName() );
                config.augment( GraphDatabaseSettings.databases_root_path, databasesRoot.getAbsolutePath() );
                TestGraphDatabaseFactoryState testState = (TestGraphDatabaseFactoryState) state;
                TestEnterpriseGraphDatabaseFacadeFactory facadeFactory = new TestEnterpriseGraphDatabaseFacadeFactory( testState, false );
                return facadeFactory.newFacade( databasesRoot, config, GraphDatabaseDependencies.newDependencies( state.databaseDependencies() ) );
            }
        };
    }

    @Override
    protected GraphDatabaseBuilder.DatabaseCreator createImpermanentDatabaseCreator( final File storeDir,
            final TestGraphDatabaseFactoryState state )
    {
        return new GraphDatabaseBuilder.DatabaseCreator()
        {
            @Override
            public GraphDatabaseService newDatabase( Config config )
            {
                return new TestEnterpriseGraphDatabaseFacadeFactory( state, true ).newFacade( storeDir, config,
                        GraphDatabaseDependencies.newDependencies( state.databaseDependencies() ) );
            }
        };
    }

    static class TestEnterpriseGraphDatabaseFacadeFactory extends TestGraphDatabaseFacadeFactory
    {
        TestEnterpriseGraphDatabaseFacadeFactory( TestGraphDatabaseFactoryState state, boolean impermanent )
        {
            super( state, impermanent, DatabaseInfo.COMMERCIAL, EnterpriseEditionModule::new );
        }

        @Override
        protected File configureAndGetDatabaseRoot( File storeDir, Config config )
        {
            return storeDir;
        }
    }

    @Override
    public String getEdition()
    {
        return Edition.COMMERCIAL.toString();
    }
}
