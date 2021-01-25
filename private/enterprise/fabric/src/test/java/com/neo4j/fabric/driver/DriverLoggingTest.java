/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.configuration.FabricEnterpriseConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.logging.Level;

import org.neo4j.configuration.Config;
import org.neo4j.driver.Logger;
import org.neo4j.fabric.executor.Location;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.logging.JULBridge;
import org.neo4j.ssl.config.SslPolicyLoader;

import static com.neo4j.fabric.TestUtils.createUri;
import static org.mockito.Mockito.mock;
import static org.neo4j.logging.AssertableLogProvider.Level.DEBUG;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

class DriverLoggingTest
{
    private static final String LOG_NAME = "Test logger";

    private static final String ERROR_MESSAGE = "ERROR MESSAGE";
    private static final String WARN_MESSAGE = "WARN MESSAGE";
    private static final String INFO_MESSAGE = "INFO MESSAGE";
    private static final String DEBUG_MESSAGE = "DEBUG MESSAGE";
    private static final String TRACE_MESSAGE = "TRACE MESSAGE";

    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Test
    void testErrorLevel()
    {
        setUp( "ERROR" );

        assertThat( logProvider ).forLevel( ERROR ).containsMessages( ERROR_MESSAGE );
    }

    @Test
    void testWarnLevel()
    {
        setUp( "WARN" );

        assertThat( logProvider ).forLevel( ERROR ).containsMessages( ERROR_MESSAGE )
                                 .forLevel( WARN ).containsMessages( WARN_MESSAGE );
    }

    @Test
    void testInfoLevel()
    {
        setUp( "INFO" );

        assertThat( logProvider )
                .forLevel( ERROR ).containsMessages( ERROR_MESSAGE )
                .forLevel( WARN ).containsMessages( WARN_MESSAGE )
                .forLevel( INFO ).containsMessages( INFO_MESSAGE );
    }

    @Test
    void testDebugLevel()
    {
        setUp( "DEBUG" );

        assertThat( logProvider )
                .forLevel( ERROR ).containsMessages( ERROR_MESSAGE )
                .forLevel( WARN ).containsMessages( WARN_MESSAGE )
                .forLevel( INFO ).containsMessages( INFO_MESSAGE )
                .forLevel( DEBUG ).containsMessages( DEBUG_MESSAGE );
    }

    @Test
    void testNoneLevel()
    {
        setUp( "NONE" );

        assertThat( logProvider ).doesNotContainMessage( ERROR_MESSAGE );
    }

    private void setUp( String configuredLevel )
    {
        var properties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", "bolt://mega:1111",
                "fabric.driver.logging.level", configuredLevel
        );
        var config = Config.newBuilder().setRaw( properties ).build();

        setUpLogging();

        var fabricConfig = FabricEnterpriseConfig.from( config );
        var driverConfigFactory = new ExternalDriverConfigFactory( fabricConfig, config, mock( SslPolicyLoader.class ) );
        var graph0DriverConfig = driverConfigFactory.createConfig( new Location.Remote.External( 0, null, createUri( "bolt://mega:1111" ), null ) );

        var logger = graph0DriverConfig.logging().getLog( LOG_NAME );
        log( logger );
    }

    private void log( Logger logger )
    {
        logger.error( ERROR_MESSAGE, null );
        logger.warn( WARN_MESSAGE );
        logger.info( INFO_MESSAGE );
        logger.debug( DEBUG_MESSAGE );
        logger.trace( TRACE_MESSAGE );
    }

    private void setUpLogging()
    {
        // this is JUL forwarding copied from ServerBootstrapper
        // in other words, this will be done by the server on start
        JULBridge.resetJUL();
        java.util.logging.Logger.getLogger( "" ).setLevel( Level.WARNING );
        JULBridge.forwardTo( logProvider );
    }
}
