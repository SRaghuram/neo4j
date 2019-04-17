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
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.factory.DatabaseManagementServiceInternalBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.TestDatabaseManagementServiceFactory;
import org.neo4j.test.TestGraphDatabaseFactoryState;

import static org.neo4j.configuration.Settings.FALSE;

public class TestCommercialDatabaseManagementServiceBuilder extends TestDatabaseManagementServiceBuilder
{
    public TestCommercialDatabaseManagementServiceBuilder()
    {
        super();
    }

    public TestCommercialDatabaseManagementServiceBuilder( LogProvider logProvider )
    {
        super( logProvider );
    }

    @Override
    protected DatabaseManagementServiceInternalBuilder.DatabaseCreator createDatabaseCreator( File storeDir,
            GraphDatabaseFactoryState state )
    {
        return config ->
        {
            augmentConfig( config );
            TestGraphDatabaseFactoryState testState = (TestGraphDatabaseFactoryState) state;
            TestCommercialDatabaseManagementServiceFactory facadeFactory = new TestCommercialDatabaseManagementServiceFactory( testState, false );
            return facadeFactory.newFacade( storeDir, config, GraphDatabaseDependencies.newDependencies( state.databaseDependencies() ) );
        };
    }

    private static void augmentConfig( Config config )
    {
        config.augment( GraphDatabaseSettings.ephemeral, FALSE );
        config.augment( OnlineBackupSettings.online_backup_listen_address, "127.0.0.1:0" );
        if ( !config.isConfigured( OnlineBackupSettings.online_backup_enabled ) )
        {
            config.augment( OnlineBackupSettings.online_backup_enabled, FALSE );
        }
    }

    @Override
    protected DatabaseManagementServiceInternalBuilder.DatabaseCreator createImpermanentDatabaseCreator( final File storeDir,
            final TestGraphDatabaseFactoryState state )
    {
        return config ->
        {
            augmentConfig( config );
            return new TestCommercialDatabaseManagementServiceFactory( state, true ).newFacade( storeDir, config,
                    GraphDatabaseDependencies.newDependencies( state.databaseDependencies() ) );
        };
    }

    @Override
    public String getEdition()
    {
        return Edition.COMMERCIAL.toString();
    }

    private static class TestCommercialDatabaseManagementServiceFactory extends TestDatabaseManagementServiceFactory
    {
        TestCommercialDatabaseManagementServiceFactory( TestGraphDatabaseFactoryState state, boolean impermanent )
        {
            super( state, impermanent, DatabaseInfo.COMMERCIAL, CommercialEditionModule::new );
        }
    }
}

