/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.configuration.FabricEnterpriseConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.driver.net.ServerAddress;
import org.neo4j.fabric.executor.Location;
import org.neo4j.ssl.config.SslPolicyLoader;

import static com.neo4j.fabric.TestUtils.createUri;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class DriverConfigFactoryTest
{

    @Test
    void testConfig()
    {
        Map<String,String> properties = new HashMap<>();
        properties.put( "fabric.database.name", "mega" );

        properties.put( "fabric.driver.logging.level", "INFO" );
        properties.put( "fabric.driver.logging.leaked_sessions", "false" );
        properties.put( "fabric.driver.connection.pool.max_size", "99" );
        properties.put( "fabric.driver.connection.pool.acquisition_timeout", "7s" );
        properties.put( "fabric.driver.connection.pool.idle_test", "19s" );
        properties.put( "fabric.driver.connection.max_lifetime", "39m" );
        properties.put( "fabric.driver.connection.connect_timeout", "3s" );

        properties.put( "fabric.driver.trust_strategy", "TRUST_SYSTEM_CA_SIGNED_CERTIFICATES" );

        properties.put( "fabric.graph.0.uri", "bolt://mega:1111" );

        // graph driver with everything overloaded
        properties.put( "fabric.graph.0.driver.logging.level", "DEBUG" );
        properties.put( "fabric.graph.0.driver.logging.leaked_sessions", "true" );
        properties.put( "fabric.graph.0.driver.connection.pool.max_size", "59" );
        properties.put( "fabric.graph.0.driver.connection.pool.acquisition_timeout", "17s" );
        properties.put( "fabric.graph.0.driver.connection.pool.idle_test", "27s" );
        properties.put( "fabric.graph.0.driver.connection.max_lifetime", "29m" );
        properties.put( "fabric.graph.0.driver.connection.connect_timeout", "9s" );

        // graph driver with nothing overloaded
        properties.put( "fabric.graph.1.uri", "bolt://mega:2222" );

        // graph driver with something overloaded
        properties.put( "fabric.graph.2.uri", "bolt://mega:3333" );
        properties.put( "fabric.graph.2.driver.connection.connect_timeout", "11" );

        var config = Config.newBuilder()
                .setRaw( properties )
                .build();

        var fabricConfig = FabricEnterpriseConfig.from( config );
        var driverConfigFactory = new ExternalDriverConfigFactory( fabricConfig, config, mock( SslPolicyLoader.class ) );

        var graph0DriverConfig = driverConfigFactory.createConfig( new Location.Remote.External( 0, null, createUri( "bolt://mega:1111" ), null ) );

        assertTrue( graph0DriverConfig.logging().getLog( "" ).isDebugEnabled() );
        assertTrue( graph0DriverConfig.logLeakedSessions() );
        assertEquals( 59, graph0DriverConfig.maxConnectionPoolSize() );
        assertEquals( Duration.ofSeconds( 27 ).toMillis(), graph0DriverConfig.idleTimeBeforeConnectionTest() );
        assertEquals( Duration.ofMinutes( 29 ).toMillis(), graph0DriverConfig.maxConnectionLifetimeMillis() );
        assertEquals( Duration.ofSeconds( 17 ).toMillis(), graph0DriverConfig.connectionAcquisitionTimeoutMillis() );
        assertEquals( Duration.ofSeconds( 9 ).toMillis(), graph0DriverConfig.connectionTimeoutMillis() );

        var graph1DriverConfig = driverConfigFactory.createConfig( new Location.Remote.External( 1, null, createUri( "bolt://mega:2222" ), null ) );

        assertFalse( graph1DriverConfig.logging().getLog( "" ).isDebugEnabled() );
        assertFalse( graph1DriverConfig.logLeakedSessions() );
        assertEquals( 99, graph1DriverConfig.maxConnectionPoolSize() );
        assertEquals( Duration.ofSeconds( 19 ).toMillis(), graph1DriverConfig.idleTimeBeforeConnectionTest() );
        assertEquals( Duration.ofMinutes( 39 ).toMillis(), graph1DriverConfig.maxConnectionLifetimeMillis() );
        assertEquals( Duration.ofSeconds( 7 ).toMillis(), graph1DriverConfig.connectionAcquisitionTimeoutMillis() );
        assertEquals( Duration.ofSeconds( 3 ).toMillis(), graph1DriverConfig.connectionTimeoutMillis() );

        var graph2DriverConfig = driverConfigFactory.createConfig( new Location.Remote.External( 2, null, createUri( "bolt://mega:3333" ), null ) );
        assertEquals( 99, graph2DriverConfig.maxConnectionPoolSize() );
        assertEquals( Duration.ofSeconds( 11 ).toMillis(), graph2DriverConfig.connectionTimeoutMillis() );
    }

    @Test
    void testDefaults()
    {
        var properties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", "bolt://mega:1111"
        );
        var config = Config.newBuilder()
                .setRaw( properties )
                .build();

        var fabricConfig = FabricEnterpriseConfig.from( config );
        var driverConfigFactory = new ExternalDriverConfigFactory( fabricConfig, config, mock( SslPolicyLoader.class ) );

        var graph0DriverConfig = driverConfigFactory.createConfig( new Location.Remote.External( 0, null, createUri( "bolt://mega:1111" ), null ) );

        assertFalse( graph0DriverConfig.logging().getLog( "" ).isDebugEnabled() );
        assertFalse( graph0DriverConfig.logLeakedSessions() );
        assertEquals( Integer.MAX_VALUE, graph0DriverConfig.maxConnectionPoolSize() );
        assertTrue( graph0DriverConfig.idleTimeBeforeConnectionTest() < 0);
        assertEquals( Duration.ofHours( 1 ).toMillis(), graph0DriverConfig.maxConnectionLifetimeMillis() );
        assertEquals( Duration.ofMinutes( 1 ).toMillis(), graph0DriverConfig.connectionAcquisitionTimeoutMillis() );
        assertEquals( Duration.ofSeconds( 5 ).toMillis(), graph0DriverConfig.connectionTimeoutMillis() );
    }

    @Test
    void testUriList()
    {
        var properties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", "bolt://core-1:1111,bolt://core-2:2222,bolt://core-3:3333"
        );
        var config = Config.newBuilder()
                .setRaw( properties )
                .build();

        var fabricConfig = FabricEnterpriseConfig.from( config );
        var driverConfigFactory = new ExternalDriverConfigFactory( fabricConfig, config, mock( SslPolicyLoader.class ) );

        var address1 = new SocketAddress( "core-1", 1111 );
        var address2 = new SocketAddress( "core-2", 2222 );
        var address3 = new SocketAddress( "core-3", 3333 );
        var uri = new Location.RemoteUri( "bolt", List.of( address1, address2, address3 ), null );
        var graph0DriverConfig = driverConfigFactory.createConfig( new Location.Remote.External( 0, null, uri, null ) );

        var resolvedAddresses = graph0DriverConfig.resolver().resolve( null );
        assertThat( resolvedAddresses ).contains( ServerAddress.of( "core-1", 1111 ), ServerAddress.of( "core-2", 2222 ), ServerAddress.of( "core-3", 3333 ) );
    }
}
