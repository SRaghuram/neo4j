/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.driver;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertTrue;

@DriverExtension
class DriverFactoryExtensionTest
{
    @Inject
    TestDirectory testDirectory;

    @Inject
    DriverFactory driverFactory;

    @Test
    void shouldCreateLogFileForDriver() throws IOException
    {
        var boltAddress = new SocketAddress( "localhost", 0 );
        var fsa = testDirectory.getFileSystem();
        DatabaseManagementService managementService = null;

        try
        {
            managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() )
                    .setFileSystem( fsa )
                    .setConfig( BoltConnector.enabled, true )
                    .setConfig( BoltConnector.listen_address, boltAddress )
                    .build();

            var bolt = ((GraphDatabaseAPI) managementService.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME ))
                    .getDependencyResolver()
                    .resolveDependency( ConnectorPortRegister.class )
                    .getLocalAddress( "bolt" );
            try ( var driver = driverFactory.graphDatabaseDriver( URI.create( "bolt://" + bolt ) ) )
            {
                driver.session().run( "RETURN 1" ).consume();
            }

            var driverLogsDir = testDirectory.homePath().resolve( DriverFactory.LOGS_DIR );
            assertTrue( fsa.isDirectory( driverLogsDir ) );
            assertTrue( fsa.fileExists( driverLogsDir.resolve( "driver-1.log" ) ) );
        }
        finally
        {
            if ( managementService != null )
            {
                managementService.shutdown();
            }
        }
    }
}
