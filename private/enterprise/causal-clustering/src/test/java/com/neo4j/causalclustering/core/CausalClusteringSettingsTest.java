/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.graphdb.config.BaseSetting;
import org.neo4j.logging.AssertableLogProvider;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.logging.AssertableLogProvider.inLog;

class CausalClusteringSettingsTest
{
    @Test
    void shouldValidatePrefixBasedKeys()
    {
        // given
        BaseSetting<String> setting = Settings.prefixSetting( "foo", Settings.STRING, "" );

        Map<String, String> rawConfig = new HashMap<>();
        rawConfig.put( "foo.us_east_1c", "abcdef" );

        // when
        Map<String, String> validConfig = setting.validate( rawConfig, s ->
        {
        } );

        // then
        assertEquals( 1, validConfig.size() );
        assertEquals( rawConfig, validConfig );
    }

    @Test
    void shouldValidateMultiplePrefixBasedKeys()
    {
        // given
        BaseSetting<String> setting = Settings.prefixSetting( "foo", Settings.STRING, "" );

        Map<String, String> rawConfig = new HashMap<>();
        rawConfig.put( "foo.us_east_1c", "abcdef" );
        rawConfig.put( "foo.us_east_1d", "ghijkl" );

        // when
        Map<String, String> validConfig = setting.validate( rawConfig, s ->
        {
        } );

        // then
        assertEquals( 2, validConfig.size() );
        assertEquals( rawConfig, validConfig );
    }

    @Test
    void shouldValidateLoadBalancingServerPolicies()
    {
        // given
        Map<String, String> rawConfig = new HashMap<>();
        rawConfig.put( "causal_clustering.load_balancing.config.server_policies.us_east_1c", "all()" );

        // when
        Map<String, String> validConfig = CausalClusteringSettings.load_balancing_config.validate( rawConfig, s ->
        {
        } );

        // then
        assertEquals( 1, validConfig.size() );
        assertEquals( rawConfig, validConfig );
    }

    @Test
    void shouldBeInvalidIfPrefixDoesNotMatch()
    {
        // given
        BaseSetting<String> setting = Settings.prefixSetting( "bar", Settings.STRING, "" );
        Map<String, String> rawConfig = new HashMap<>();
        rawConfig.put( "foo.us_east_1c", "abcdef" );

        // when
        Map<String, String> validConfig = setting.validate( rawConfig, s ->
        {
        } );

        // then
        assertEquals( 0, validConfig.size() );
    }

    @Test
    void shouldMigrateOldRoutingTtlSetting()
    {
        testRoutingTtlSettingMigration( "42m", Duration.ofMinutes( 42 ) );
    }

    @Test
    void shouldNotMigrateOldRoutingTtlSettingWhenEmpty()
    {
        testRoutingTtlSettingMigration( "", Duration.ofSeconds( 300 ) );
    }

    private void testRoutingTtlSettingMigration( String rawValue, Duration expectedValue )
    {
        Config config = Config.builder()
                .withSetting( "causal_clustering.cluster_routing_ttl", rawValue )
                .build();

        AssertableLogProvider logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertFalse( config.getRaw( "causal_clustering.cluster_routing_ttl" ).isPresent(), "Old TTL setting should be absent" );
        assertEquals( expectedValue, config.get( GraphDatabaseSettings.routing_ttl ) );

        logProvider.assertAtLeastOnce(
                inLog( Config.class ).warn( containsString( "Deprecated configuration options used" ) ) );
        logProvider.assertAtLeastOnce(
                inLog( Config.class ).warn( containsString( "causal_clustering.cluster_routing_ttl has been replaced with dbms.routing_ttl" ) ) );
    }
}
