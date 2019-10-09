/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.fabric.config.FabricConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.Config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Config.LoadBalancingStrategy.LEAST_CONNECTED;
import static org.neo4j.driver.Config.LoadBalancingStrategy.ROUND_ROBIN;
import static org.neo4j.driver.Config.TrustStrategy.Strategy.TRUST_ALL_CERTIFICATES;
import static org.neo4j.driver.Config.TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES;

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
        properties.put( "fabric.driver.connection.encrypted", "true" );

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
        properties.put( "fabric.graph.0.driver.connection.encrypted", "false" );
        properties.put( "fabric.graph.0.driver.trust_strategy", "TRUST_ALL_CERTIFICATES" );

        // graph driver with nothing overloaded
        properties.put( "fabric.graph.1.uri", "bolt://mega:2222" );

        // graph driver with something overloaded
        properties.put( "fabric.graph.2.uri", "bolt://mega:333" );
        properties.put( "fabric.graph.2.driver.connection.connect_timeout", "11" );

        var config = Config.newBuilder()
                .setRaw( properties )
                .build();

        var fabricConfig = FabricConfig.from( config );
        var driverConfigFactory = new DriverConfigFactory( fabricConfig, config );

        var graph0DriverConfig = driverConfigFactory.createConfig( getGraph( fabricConfig, 0 ) );

        assertTrue( graph0DriverConfig.logging().getLog( "" ).isDebugEnabled() );
        assertTrue( graph0DriverConfig.logLeakedSessions() );
        assertEquals( 59, graph0DriverConfig.maxConnectionPoolSize() );
        assertEquals( Duration.ofSeconds( 27 ).toMillis(), graph0DriverConfig.idleTimeBeforeConnectionTest() );
        assertEquals( Duration.ofMinutes( 29 ).toMillis(), graph0DriverConfig.maxConnectionLifetimeMillis() );
        assertEquals( Duration.ofSeconds( 17 ).toMillis(), graph0DriverConfig.connectionAcquisitionTimeoutMillis() );
        assertFalse( graph0DriverConfig.encrypted() );
        assertEquals( TRUST_ALL_CERTIFICATES, graph0DriverConfig.trustStrategy().strategy() );
        assertEquals( Duration.ofSeconds( 9 ).toMillis(), graph0DriverConfig.connectionTimeoutMillis() );

        var graph1DriverConfig = driverConfigFactory.createConfig( getGraph( fabricConfig, 1 ) );

        assertFalse( graph1DriverConfig.logging().getLog( "" ).isDebugEnabled() );
        assertFalse( graph1DriverConfig.logLeakedSessions() );
        assertEquals( 99, graph1DriverConfig.maxConnectionPoolSize() );
        assertEquals( Duration.ofSeconds( 19 ).toMillis(), graph1DriverConfig.idleTimeBeforeConnectionTest() );
        assertEquals( Duration.ofMinutes( 39 ).toMillis(), graph1DriverConfig.maxConnectionLifetimeMillis() );
        assertEquals( Duration.ofSeconds( 7 ).toMillis(), graph1DriverConfig.connectionAcquisitionTimeoutMillis() );
        assertTrue( graph1DriverConfig.encrypted() );
        assertEquals( TRUST_SYSTEM_CA_SIGNED_CERTIFICATES, graph1DriverConfig.trustStrategy().strategy() );
        assertEquals( Duration.ofSeconds( 3 ).toMillis(), graph1DriverConfig.connectionTimeoutMillis() );

        var graph2DriverConfig = driverConfigFactory.createConfig( getGraph( fabricConfig, 2 ) );
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

        var fabricConfig = FabricConfig.from( config );
        var driverConfigFactory = new DriverConfigFactory( fabricConfig, config );

        var graph0DriverConfig = driverConfigFactory.createConfig( getGraph( fabricConfig, 0 ) );

        assertFalse( graph0DriverConfig.logging().getLog( "" ).isDebugEnabled() );
        assertFalse( graph0DriverConfig.logLeakedSessions() );
        assertEquals( Integer.MAX_VALUE, graph0DriverConfig.maxConnectionPoolSize() );
        assertTrue( graph0DriverConfig.idleTimeBeforeConnectionTest() < 0);
        assertEquals( Duration.ofHours( 1 ).toMillis(), graph0DriverConfig.maxConnectionLifetimeMillis() );
        assertEquals( Duration.ofMinutes( 1 ).toMillis(), graph0DriverConfig.connectionAcquisitionTimeoutMillis() );
        assertEquals( true, graph0DriverConfig.encrypted() );
        assertEquals( TRUST_ALL_CERTIFICATES, graph0DriverConfig.trustStrategy().strategy() );
        assertEquals( Duration.ofSeconds( 5 ).toMillis(), graph0DriverConfig.connectionTimeoutMillis() );
    }

    private FabricConfig.Graph getGraph( FabricConfig fabricConfig, long id )
    {
        return fabricConfig.getDatabase().getGraphs().stream()
                .filter( graph -> graph.getId() == id )
                .findAny()
                .orElseThrow( () -> new IllegalStateException( "Graph with id " + id + " not found" ) );
    }
}
