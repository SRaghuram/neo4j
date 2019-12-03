/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.ports.PortAuthority;
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
        var boltAddress = new SocketAddress( "localhost", PortAuthority.allocatePort() );
        var fsa = testDirectory.getFileSystem();
        DatabaseManagementService managementService = null;

        try
        {
            managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                    .setFileSystem( fsa )
                    .setConfig( BoltConnector.enabled, true )
                    .setConfig( BoltConnector.listen_address, boltAddress )
                    .build();

            try ( var driver = driverFactory.graphDatabaseDriver( URI.create( "bolt://" + boltAddress.getHostname() + ":" + boltAddress.getPort() ) ) )
            {
                driver.session().run( "RETURN 1" ).consume();
            }

            var driverLogsDir = new File( testDirectory.homeDir(), DriverFactory.LOGS_DIR );
            assertTrue( fsa.isDirectory( driverLogsDir ) );
            assertTrue( fsa.fileExists( new File( driverLogsDir, "driver-1.log" ) ) );
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
