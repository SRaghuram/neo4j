/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise.jmx;

import com.neo4j.server.enterprise.CommercialNeoServer;
import com.neo4j.server.enterprise.helpers.CommercialServerBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.NeoServer;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class ServerManagementIT
{
    @Inject
    private TestDirectory baseDir;

    @Test
    void shouldBeAbleToRestartServer() throws Exception
    {
        // Given
        String dataDirectory1 = baseDir.directory( "data1" ).getAbsolutePath();
        String dataDirectory2 = baseDir.directory( "data2" ).getAbsolutePath();

        Config config = Config.fromFile( CommercialServerBuilder
                    .serverOnRandomPorts()
                    .withDefaultDatabaseTuning()
                    .usingDataDir( dataDirectory1 )
                    .createConfigFiles() )
                .withHome( baseDir.directory() )
                .withSetting( logs_directory, baseDir.directory( "logs" ).getPath() )
                .build();

        // When
        NeoServer server = new CommercialNeoServer( config, graphDbDependencies() );
        try
        {
            server.start();

            assertNotNull( server.getDatabase().getGraph() );

            // Change the database location
            config.augment( data_directory, dataDirectory2 );
            ServerManagement bean = new ServerManagement( server );
            bean.restartServer();

            // Then
            assertNotNull( server.getDatabase().getGraph() );
        }
        finally
        {
            server.stop();
        }
    }

    private static GraphDatabaseDependencies graphDbDependencies()
    {
        return GraphDatabaseDependencies.newDependencies().userLogProvider( NullLogProvider.getInstance() );
    }
}
