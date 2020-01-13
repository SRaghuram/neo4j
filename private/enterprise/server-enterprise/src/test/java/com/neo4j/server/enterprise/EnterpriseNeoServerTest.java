/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.Mode;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.mode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
class EnterpriseNeoServerTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void checkExpectedDatabaseDirectory()
    {
        Config config = Config.newBuilder()
                .setDefaults( GraphDatabaseSettings.SERVER_DEFAULTS )
                .set( mode, Mode.SINGLE )
                .set( GraphDatabaseSettings.neo4j_home, testDirectory.homeDir().toPath().toAbsolutePath() )
                .set( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) )
                .set( HttpConnector.listen_address, new SocketAddress( "localhost", 0 ) )
                .set( HttpsConnector.listen_address, new SocketAddress( "localhost", 0 ) )
                .set( GraphDatabaseSettings.pagecache_memory, "8m" )
                .set( OnlineBackupSettings.online_backup_listen_address, new SocketAddress( "127.0.0.1", 0 ) )
                .set( OnlineBackupSettings.online_backup_enabled, false )
                .set( GraphDatabaseSettings.logical_log_rotation_threshold, ByteUnit.kibiBytes( 128 ) )
                .build();
        GraphDatabaseDependencies dependencies = GraphDatabaseDependencies.newDependencies().userLogProvider( NullLogProvider.getInstance() );
        EnterpriseNeoServer server = new EnterpriseNeoServer( config, dependencies );

        server.start();
        try
        {
            Path expectedPath = Paths.get( testDirectory.homeDir().getPath(), "data", "databases", DEFAULT_DATABASE_NAME );
            GraphDatabaseFacade graph = server.getDatabaseService().getDatabase();
            assertEquals( expectedPath, graph.databaseLayout().databaseDirectory().toPath() );
        }
        finally
        {
            server.stop();
        }
    }
}
