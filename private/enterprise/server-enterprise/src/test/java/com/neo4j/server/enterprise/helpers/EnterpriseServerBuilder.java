/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise.helpers;

import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.server.database.EnterpriseGraphFactory;
import com.neo4j.server.enterprise.EnterpriseNeoServer;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.helpers.CommunityServerBuilder;

import static org.neo4j.configuration.SettingValueParsers.FALSE;

public class EnterpriseServerBuilder extends CommunityServerBuilder
{
    protected EnterpriseServerBuilder( LogProvider logProvider )
    {
        super( logProvider );
    }

    public static EnterpriseServerBuilder server()
    {
        return server( NullLogProvider.getInstance() );
    }

    public static EnterpriseServerBuilder serverOnRandomPorts()
    {
        EnterpriseServerBuilder server = server();
        server.onRandomPorts();
        server.withProperty( BoltConnector.listen_address.name(), "localhost:0" );
        server.withProperty( OnlineBackupSettings.online_backup_listen_address.name(), "127.0.0.1:0" );
        server.withProperty( OnlineBackupSettings.online_backup_enabled.name(), FALSE );
        return server;
    }

    public static EnterpriseServerBuilder server( LogProvider logProvider )
    {
        return new EnterpriseServerBuilder( logProvider );
    }

    @Override
    public EnterpriseNeoServer build() throws IOException
    {
        return (EnterpriseNeoServer) super.build();
    }

    @Override
    public EnterpriseServerBuilder usingDataDir( String dataDir )
    {
        super.usingDataDir( dataDir );
        return this;
    }

    @Override
    protected CommunityNeoServer build( File configFile, Config config,
            ExternalDependencies dependencies )
    {
        return new TestEnterpriseNeoServer( config, configFile, isPersistent(),
                                            GraphDatabaseDependencies.newDependencies(dependencies).userLogProvider( logProvider ) );
    }

    private class TestEnterpriseNeoServer extends EnterpriseNeoServer
    {
        private final File configFile;

        TestEnterpriseNeoServer( Config config, File configFile, boolean persistent, ExternalDependencies dependencies )
        {
            super( config, persistent ? new EnterpriseGraphFactory() : new EnterpriseInMemoryGraphFactory(), dependencies );
            this.configFile = configFile;
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
        configuration.put( OnlineBackupSettings.online_backup_enabled.name(), FALSE );

        return configuration;
    }
}
