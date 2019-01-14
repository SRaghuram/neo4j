/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise.helpers;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.server.enterprise.CommercialNeoServer;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.rest.web.DatabaseActions;

public class CommercialServerBuilder extends CommunityServerBuilder
{
    protected CommercialServerBuilder( LogProvider logProvider )
    {
        super( logProvider );
    }

    public static CommercialServerBuilder server()
    {
        return server( NullLogProvider.getInstance() );
    }

    public static CommercialServerBuilder serverOnRandomPorts()
    {
        CommercialServerBuilder server = server();
        server.onRandomPorts();
        server.withProperty( new BoltConnector( "bolt" ).listen_address.name(), "localhost:0" );
        server.withProperty( OnlineBackupSettings.online_backup_listen_address.name(), "127.0.0.1:0" );
        server.withProperty( OnlineBackupSettings.online_backup_enabled.name(), Settings.FALSE );
        return server;
    }

    public static CommercialServerBuilder server( LogProvider logProvider )
    {
        return new CommercialServerBuilder( logProvider );
    }

    @Override
    public CommercialNeoServer build() throws IOException
    {
        return (CommercialNeoServer) super.build();
    }

    @Override
    public CommercialServerBuilder usingDataDir( String dataDir )
    {
        super.usingDataDir( dataDir );
        return this;
    }

    @Override
    protected CommunityNeoServer build( File configFile, Config config,
            GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        return new TestCommercialNeoServer( config, configFile,
                GraphDatabaseDependencies.newDependencies(dependencies).userLogProvider( logProvider ) );
    }

    private class TestCommercialNeoServer extends CommercialNeoServer
    {
        private final File configFile;

        TestCommercialNeoServer( Config config, File configFile, GraphDatabaseFacadeFactory.Dependencies dependencies )
        {
            super( config, dependencies );
            this.configFile = configFile;
        }

        @Override
        protected DatabaseActions createDatabaseActions()
        {
            return createDatabaseActionsObject( database );
        }

        @Override
        public void stop()
        {
            super.stop();
            if ( configFile != null )
            {
                configFile.delete();
            }
        }
    }

    @Override
    public Map<String, String> createConfiguration( File temporaryFolder )
    {
        Map<String, String> configuration = super.createConfiguration( temporaryFolder );

        configuration.put( OnlineBackupSettings.online_backup_listen_address.name(), "127.0.0.1:0" );
        configuration.putIfAbsent( MetricsSettings.csvPath.name(), new File( temporaryFolder, "metrics" ).getAbsolutePath() );
        configuration.put( OnlineBackupSettings.online_backup_enabled.name(), Settings.FALSE );

        return configuration;
    }
}
