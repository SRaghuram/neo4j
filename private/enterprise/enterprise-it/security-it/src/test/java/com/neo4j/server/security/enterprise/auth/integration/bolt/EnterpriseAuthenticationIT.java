/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.integration.bolt;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.transport.AuthenticationIT;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class EnterpriseAuthenticationIT extends AuthenticationIT
{
    @Override
    protected TestDatabaseManagementServiceBuilder getTestGraphDatabaseFactory()
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder().setUserLogProvider( logProvider );
    }

    @Override
    protected Consumer<Map<Setting<?>, Object>> getSettingsFunction()
    {
        final Path homeDir;
        try
        {
            homeDir = Files.createTempDirectory( "logs" );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Test setup failed to create temporary directory", e );
        }

        return settings ->
        {
            super.getSettingsFunction().accept( settings );
            settings.put( GraphDatabaseSettings.logs_directory, homeDir.toAbsolutePath() );
        };
    }

    @Override
    public void shouldFailIfMalformedAuthTokenUnknownScheme( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name )
    {
        // Ignore this test in enterprise since custom schemes may be allowed
    }
}
