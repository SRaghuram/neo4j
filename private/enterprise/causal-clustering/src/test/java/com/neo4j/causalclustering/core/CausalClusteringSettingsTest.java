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
import java.util.Objects;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.BaseSetting;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Level;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.load_balancing_config;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.middleware_logging_level;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.configuration.Settings.FALSE;
import static org.neo4j.configuration.Settings.STRING;
import static org.neo4j.configuration.Settings.TRUE;
import static org.neo4j.configuration.Settings.prefixSetting;
import static org.neo4j.logging.AssertableLogProvider.inLog;

class CausalClusteringSettingsTest
{
    @Test
    void shouldValidatePrefixBasedKeys()
    {
        // given
        BaseSetting<String> setting = prefixSetting( "foo", STRING, "" );

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
        BaseSetting<String> setting = prefixSetting( "foo", STRING, "" );

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
        Map<String,String> validConfig = load_balancing_config.validate( rawConfig, s ->
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
        BaseSetting<String> setting = prefixSetting( "bar", STRING, "" );
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

    @Test
    void shouldMigrateMiddlewareLoggingLevelFromIntegerToLevel()
    {
        testMiddlewareLoggingLevelMigration( java.util.logging.Level.OFF, Level.NONE );
        testMiddlewareLoggingLevelMigration( java.util.logging.Level.FINE, Level.DEBUG );
        testMiddlewareLoggingLevelMigration( java.util.logging.Level.FINER, Level.DEBUG );
        testMiddlewareLoggingLevelMigration( java.util.logging.Level.INFO, Level.INFO );
        testMiddlewareLoggingLevelMigration( java.util.logging.Level.WARNING, Level.WARN );
        testMiddlewareLoggingLevelMigration( java.util.logging.Level.SEVERE, Level.ERROR );
    }

    @Test
    void shouldNotMigrateMiddlewareLoggingLevelWhenUpToDate()
    {
        var setting = middleware_logging_level;

        var config = Config.builder()
                .withSetting( setting, Level.INFO.toString() )
                .build();

        assertEquals( Level.INFO, config.get( setting ) );

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        logProvider.assertNoLoggingOccurred();
    }

    @Test
    void shouldMigrateDisableMiddlewareLoggingSettingWhenTrue()
    {
        testDisableMiddlewareLoggingMigration( TRUE, Level.NONE, Level.NONE );
        testDisableMiddlewareLoggingMigration( TRUE, Level.INFO, Level.NONE );
        testDisableMiddlewareLoggingMigration( TRUE, Level.DEBUG, Level.NONE );

        testDisableMiddlewareLoggingMigration( FALSE, Level.DEBUG, Level.DEBUG );
        testDisableMiddlewareLoggingMigration( FALSE, Level.ERROR, Level.ERROR );
        testDisableMiddlewareLoggingMigration( FALSE, Level.INFO, Level.INFO );

        testDisableMiddlewareLoggingMigration( null, Level.INFO, Level.INFO );
        testDisableMiddlewareLoggingMigration( null, Level.WARN, Level.WARN );
        testDisableMiddlewareLoggingMigration( null, Level.NONE, Level.NONE );
    }

    private static void testRoutingTtlSettingMigration( String rawValue, Duration expectedValue )
    {
        var config = Config.builder()
                .withSetting( "causal_clustering.cluster_routing_ttl", rawValue )
                .build();

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertFalse( config.getRaw( "causal_clustering.cluster_routing_ttl" ).isPresent(), "Old TTL setting should be absent" );
        assertEquals( expectedValue, config.get( GraphDatabaseSettings.routing_ttl ) );

        logProvider.assertAtLeastOnce(
                inLog( Config.class ).warn( containsString( "Deprecated configuration options used" ) ) );
        logProvider.assertAtLeastOnce(
                inLog( Config.class ).warn( containsString( "causal_clustering.cluster_routing_ttl has been replaced with dbms.routing_ttl" ) ) );
    }

    private static void testMiddlewareLoggingLevelMigration( java.util.logging.Level julLevel, Level neo4jLevel )
    {
        var setting = middleware_logging_level;

        var config = Config.builder()
                .withSetting( setting, String.valueOf( julLevel.intValue() ) )
                .build();

        assertEquals( neo4jLevel, config.get( setting ) );

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        logProvider.assertAtLeastOnce(
                inLog( Config.class ).warn( containsString( "Deprecated configuration options used" ) ) );
        logProvider.assertAtLeastOnce(
                inLog( Config.class ).warn( containsString( setting.name() + " with integer value has been changed to use logging levels" ) ) );
    }

    private static void testDisableMiddlewareLoggingMigration( String rawValue, Level configuredLevel, Level expectedLevel )
    {
        var settingName = "causal_clustering.disable_middleware_logging";

        var config = Config.builder()
                .withSetting( settingName, rawValue )
                .withSetting( middleware_logging_level, configuredLevel.toString() )
                .build();

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertFalse( config.getRaw( settingName ).isPresent(), "Disable middleware logging setting should be absent" );
        assertEquals( expectedLevel, config.get( middleware_logging_level ) );

        if ( Objects.equals( TRUE, rawValue ) )
        {
            logProvider.assertAtLeastOnce(
                    inLog( Config.class ).warn( containsString( "Deprecated configuration options used" ) ) );
            logProvider.assertAtLeastOnce(
                    inLog( Config.class ).warn( containsString( settingName + " has been removed" ) ) );
        }
    }
}
