/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test;

import com.neo4j.commercial.edition.CommercialEditionModule;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.Edition;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactoryState;

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
                config.augment( GraphDatabaseSettings.ephemeral, Settings.FALSE );
                config.augment( GraphDatabaseSettings.active_database, storeDir.getName() );
                config.augment( GraphDatabaseSettings.databases_root_path, databasesRoot.getAbsolutePath() );
                return new GraphDatabaseFacadeFactory( DatabaseInfo.ENTERPRISE, CommercialEditionModule::new )
                {
                    @Override
                    protected PlatformModule createPlatform( File storeDir, Config config, Dependencies dependencies )
                    {
                        return new PlatformModule( storeDir, config, databaseInfo, dependencies )
                        {
                            @Override
                            protected LogService createLogService( LogProvider userLogProvider )
                            {
                                if ( state instanceof TestGraphDatabaseFactoryState )
                                {
                                    LogProvider logProvider = ((TestGraphDatabaseFactoryState) state).getInternalLogProvider();
                                    if ( logProvider != null )
                                    {
                                        return new SimpleLogService( logProvider );
                                    }
                                }
                                return super.createLogService( userLogProvider );
                            }
                        };
                    }
                }.newFacade( databasesRoot, config, GraphDatabaseDependencies.newDependencies( state.databaseDependencies() ) );
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
                return new TestCommercialGraphDatabaseFacadeFactory( state, true ).newFacade( storeDir, config,
                        GraphDatabaseDependencies.newDependencies( state.databaseDependencies() ) );
            }
        };
    }

    static class TestCommercialGraphDatabaseFacadeFactory extends TestGraphDatabaseFacadeFactory
    {

        TestCommercialGraphDatabaseFacadeFactory( TestGraphDatabaseFactoryState state, boolean impermanent )
        {
            super( state, impermanent, DatabaseInfo.ENTERPRISE, CommercialEditionModule::new );
        }
    }

    @Override
    public String getEdition()
    {
        return Edition.enterprise.toString();
    }
}

