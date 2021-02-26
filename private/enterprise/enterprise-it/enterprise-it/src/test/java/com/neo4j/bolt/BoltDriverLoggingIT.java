/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.test.driver.DriverExtension;
import com.neo4j.test.driver.DriverFactory;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.logging.Level;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;

/*
These tests are mostly to check that getting logs from a DriverFactory instance works correctly rather than to test that driver logging works
 */
@EnterpriseDbmsExtension( configurationCallback = "configureNeo4j" )
@DriverExtension
@Execution( ExecutionMode.SAME_THREAD )
public class BoltDriverLoggingIT
{
    @Inject
    private ConnectorPortRegister connectorPortRegister;

    @ExtensionCallback
    void configureNeo4j( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( BoltConnector.enabled, true );
        builder.setConfig( BoltConnector.listen_address, new SocketAddress( 0 ) );
        builder.setConfig( auth_enabled, false );
    }

    @Inject
    private DriverFactory driverFactory;

    @Test
    public void shouldLog() throws IOException
    {
        // given
        var driverConfig = DriverFactory.instanceConfig().withLogLevel( Level.DEBUG );
        HostnamePort boltAddress = connectorPortRegister.getLocalAddress( BoltConnector.NAME );
        var uri = URI.create( "neo4j://" + boltAddress.toString( "localhost" ) );
        assertThat( uri ).hasScheme( "neo4j" );

        // when
        Path logFile;
        try ( var driver = driverFactory.graphDatabaseDriver( uri, driverConfig );
              var session = driver.session() )
        {
            for ( int i = 0; i < 5; i++ )
            {
                session.run( "RETURN 1 AS num" ).consume();
            }
            logFile = driverFactory.getLogFile( driver );
        }

        // then
        String logs = Files.readString( logFile );

        assertThat( logs ).as( "should log bolt protocol messages" )
                          .contains( "HELLO",
                                     "SUCCESS",
                                     "RUN",
                                     "PULL",
                                     "RECORD",
                                     "RESET"
                          );

        assertThat( logs ).as( "should contain connection info" )
                          .contains( ":" + boltAddress.getPort() );

        assertThat( logs ).as( "should contain queries" )
                          .contains( "RETURN 1 AS num" );
    }

    @Test
    public void shouldNotLogAnythingAtWarnOrError() throws IOException
    {
        // given
        var driverConfig = DriverFactory.instanceConfig().withLogLevel( Level.WARN );
        HostnamePort boltAddress = connectorPortRegister.getLocalAddress( BoltConnector.NAME );
        var uri = URI.create( "neo4j://" + boltAddress.toString( "localhost" ) );
        assertThat( uri ).hasScheme( "neo4j" );

        // when
        Path logFile;
        try ( var driver = driverFactory.graphDatabaseDriver( uri, driverConfig );
              var session = driver.session() )
        {
            for ( int i = 0; i < 5; i++ )
            {
                session.run( "RETURN 1 AS num" ).consume();
            }
            logFile = driverFactory.getLogFile( driver );
        }

        // then
        String logs = Files.readString( logFile );
        assertThat( logs ).isEmpty();
    }
}
