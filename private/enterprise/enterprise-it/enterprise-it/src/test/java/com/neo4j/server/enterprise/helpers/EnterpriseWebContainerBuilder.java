/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise.helpers;

import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;

import java.io.File;
import java.util.Map;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.helpers.CommunityWebContainerBuilder;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.neo4j.configuration.SettingValueParsers.FALSE;

public class EnterpriseWebContainerBuilder extends CommunityWebContainerBuilder
{
    protected EnterpriseWebContainerBuilder( LogProvider logProvider )
    {
        super( logProvider );
    }

    public static EnterpriseWebContainerBuilder builder()
    {
        return builder( NullLogProvider.getInstance() );
    }

    public static EnterpriseWebContainerBuilder builderOnRandomPorts()
    {
        EnterpriseWebContainerBuilder server = builder();
        server.onRandomPorts();
        server.withProperty( BoltConnector.listen_address.name(), "localhost:0" );
        server.withProperty( OnlineBackupSettings.online_backup_listen_address.name(), "127.0.0.1:0" );
        server.withProperty( OnlineBackupSettings.online_backup_enabled.name(), FALSE );
        return server;
    }

    public static EnterpriseWebContainerBuilder builder( LogProvider logProvider )
    {
        return new EnterpriseWebContainerBuilder( logProvider );
    }

    @Override
    public EnterpriseWebContainerBuilder usingDataDir( String dataDir )
    {
        super.usingDataDir( dataDir );
        return this;
    }

    @Override
    protected TestDatabaseManagementServiceBuilder createManagementServiceBuilder()
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder();
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
