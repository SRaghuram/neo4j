/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.DurationRange;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Level;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.discovery_advertised_address;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.discovery_listen_address;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.failure_detection_window;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.failure_resolution_window;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.middleware_logging_level;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_advertised_address;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_listen_address;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.transaction_advertised_address;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.transaction_listen_address;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.neo4j.configuration.GraphDatabaseSettings.routing_ttl;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.configuration.helpers.DurationRange.fromSeconds;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

class CausalClusteringSettingsMigratorTest
{
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
    void shouldMigrateMiddlewareLoggingLevelFromIntegerToLevelAndNewSettingName()
    {
        testMiddlewareLoggingLevelMigrationFromOldSetting( java.util.logging.Level.OFF, Level.NONE );
        testMiddlewareLoggingLevelMigrationFromOldSetting( java.util.logging.Level.FINE, Level.DEBUG );
        testMiddlewareLoggingLevelMigrationFromOldSetting( java.util.logging.Level.FINER, Level.DEBUG );
        testMiddlewareLoggingLevelMigrationFromOldSetting( java.util.logging.Level.INFO, Level.INFO );
        testMiddlewareLoggingLevelMigrationFromOldSetting( java.util.logging.Level.WARNING, Level.WARN );
        testMiddlewareLoggingLevelMigrationFromOldSetting( java.util.logging.Level.SEVERE, Level.ERROR );
    }

    @Test
    void shouldMigrateMiddlewareLoggingLevelFromIntegerToLevelWithExistingSettingName()
    {
        testMiddlewareLoggingLevelMigrationFromNewSetting( java.util.logging.Level.OFF, Level.NONE );
        testMiddlewareLoggingLevelMigrationFromNewSetting( java.util.logging.Level.FINE, Level.DEBUG );
        testMiddlewareLoggingLevelMigrationFromNewSetting( java.util.logging.Level.FINER, Level.DEBUG );
        testMiddlewareLoggingLevelMigrationFromNewSetting( java.util.logging.Level.INFO, Level.INFO );
        testMiddlewareLoggingLevelMigrationFromNewSetting( java.util.logging.Level.WARNING, Level.WARN );
        testMiddlewareLoggingLevelMigrationFromNewSetting( java.util.logging.Level.SEVERE, Level.ERROR );
    }

    @Test
    void shouldNotMigrateMiddlewareLoggingLevelWhenUpToDate()
    {
        var setting = middleware_logging_level;

        var config = Config.defaults( setting, Level.INFO );

        assertEquals( Level.INFO, config.get( setting ) );

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertThat(logProvider).doesNotHaveAnyLogs();
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

    @TestFactory
    Collection<DynamicTest> testAddressMigration()
    {
        Collection<DynamicTest> tests = new ArrayList<>();
        tests.add( dynamicTest( "Test transaction address migration", () -> testAddrMigration( transaction_listen_address, transaction_advertised_address ) ) );
        tests.add( dynamicTest( "Test raft address migration", () -> testAddrMigration( raft_listen_address, raft_advertised_address ) ) );
        tests.add( dynamicTest( "Test discovery address migration", () -> testAddrMigration( discovery_listen_address, discovery_advertised_address ) ) );
        return tests;
    }

    @Test
    void shouldMigrateElectionTimeout()
    {
        testFailureWindows( "10s", fromSeconds( 10, 10 + CausalClusteringSettingsMigrator.DEFAULT_FAILURE_DETECTION_MAX_WINDOW_IN_SECONDS ),
                            failure_resolution_window.defaultValue() );
        testFailureWindows( "4s", fromSeconds( 4, 8 ), failure_resolution_window.defaultValue() );
        testFailureWindows( "1s", fromSeconds( 1, 2 ), fromSeconds( 1, 2 ) );
    }

    @Test
    void shouldIgnoreElectionTimeout()
    {
        var setting = "causal_clustering.leader_election_timeout";
        var value = fromSeconds( 10, 12 );
        var config = Config.newBuilder().setRaw( Map.of( setting, "1s", failure_detection_window.name(), value.valueToString() ) ).build();

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertEquals( value, config.get( failure_detection_window ) );

        assertThat(logProvider).forClass( Config.class ).forLevel( WARN )
                               .containsMessageWithArguments( "Deprecated setting '%s' is ignored because replacement '%s' and '%s' is set",
                                                              setting, failure_detection_window.name(), failure_resolution_window.name() );
    }

    private static void testAddrMigration( Setting<SocketAddress> listenAddr, Setting<SocketAddress> advertisedAddr )
    {
        Config config1 = Config.newBuilder().setRaw( Map.of( listenAddr.name(), "foo:111" ) ).build();
        Config config2 = Config.newBuilder().setRaw( Map.of( listenAddr.name(), ":222" ) ).build();
        Config config3 = Config.newBuilder().setRaw( Map.of( listenAddr.name(), ":333", advertisedAddr.name(), "bar" ) ).build();
        Config config4 = Config.newBuilder().setRaw( Map.of( listenAddr.name(), "foo:444", advertisedAddr.name(), ":555" ) ).build();
        Config config5 = Config.newBuilder().setRaw( Map.of( listenAddr.name(), "foo", advertisedAddr.name(), "bar" ) ).build();
        Config config6 = Config.newBuilder().setRaw( Map.of( listenAddr.name(), "foo:666", advertisedAddr.name(), "bar:777" ) ).build();

        var logProvider = new AssertableLogProvider();
        config1.setLogger( logProvider.getLog( Config.class ) );
        config2.setLogger( logProvider.getLog( Config.class ) );
        config3.setLogger( logProvider.getLog( Config.class ) );
        config4.setLogger( logProvider.getLog( Config.class ) );
        config5.setLogger( logProvider.getLog( Config.class ) );
        config6.setLogger( logProvider.getLog( Config.class ) );

        assertEquals( new SocketAddress( "localhost", 111 ), config1.get( advertisedAddr ) );
        assertEquals( new SocketAddress( "localhost", 222 ), config2.get( advertisedAddr ) );
        assertEquals( new SocketAddress( "bar", 333 ), config3.get( advertisedAddr ) );
        assertEquals( new SocketAddress( "localhost", 555 ), config4.get( advertisedAddr ) );
        assertEquals( new SocketAddress( "bar", advertisedAddr.defaultValue().getPort() ), config5.get( advertisedAddr ) );
        assertEquals( new SocketAddress( "bar", 777 ), config6.get( advertisedAddr ) );

        String msg = "Note that since you did not explicitly set the port in %s Neo4j automatically set it to %s to match %s." +
                " This behavior may change in the future and we recommend you to explicitly set it.";

        var warnMatcher = assertThat( logProvider ).forClass( Config.class ).forLevel( WARN );
        var infoMatcher = assertThat( logProvider ).forClass( Config.class ).forLevel( INFO );
        infoMatcher.containsMessageWithArguments( msg, advertisedAddr.name(), 111, listenAddr.name() );
        infoMatcher.containsMessageWithArguments( msg, advertisedAddr.name(), 222, listenAddr.name() );
        warnMatcher.containsMessageWithArguments( msg, advertisedAddr.name(), 333, listenAddr.name() );

        warnMatcher.doesNotContainMessageWithArguments( msg, advertisedAddr.name(), 444, listenAddr.name() );
        infoMatcher.doesNotContainMessageWithArguments( msg, advertisedAddr.name(), 555, listenAddr.name() );
        warnMatcher.doesNotContainMessageWithArguments( msg, advertisedAddr.name(), 666, listenAddr.name() );
    }

    private static void testRoutingTtlSettingMigration( String rawValue, Duration expectedValue )
    {
        String setting = "causal_clustering.cluster_routing_ttl";
        var config = Config.newBuilder().setRaw( Map.of( setting, rawValue ) ).build();

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertThrows( IllegalArgumentException.class, () -> config.getSetting( setting ) );
        assertEquals( expectedValue, config.get( routing_ttl ) );

        if ( StringUtils.isNotEmpty( rawValue ) )
        {
            assertThat(logProvider).forClass( Config.class ).forLevel( WARN )
                    .containsMessageWithArguments( "Use of deprecated setting %s. It is replaced by %s", setting, routing_ttl.name() );
        }
    }

    private static void testMiddlewareLoggingLevelMigrationFromNewSetting( java.util.logging.Level julLevel, Level neo4jLevel )
    {
        var setting = middleware_logging_level;
        var oldValue = String.valueOf( julLevel.intValue() );
        var config = Config.newBuilder().setRaw( Map.of( setting.name(), oldValue ) ).build();

        assertEquals( neo4jLevel, config.get( setting ) );

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertThat(logProvider).forClass( Config.class ).forLevel( WARN )
                .containsMessageWithArguments( "Old value format in %s used. %s migrated to %s",
                        middleware_logging_level.name(), oldValue, neo4jLevel.toString() );
    }

    private static void testMiddlewareLoggingLevelMigrationFromOldSetting( java.util.logging.Level julLevel, Level neo4jLevel )
    {
        var oldSettingName = "causal_clustering.middleware_logging.level";
        var oldValue = String.valueOf( julLevel.intValue() );
        var config = Config.newBuilder().setRaw( MapUtil.genericMap( oldSettingName, oldValue ) ).build();

        assertThrows( IllegalArgumentException.class, () -> config.getSetting( oldSettingName ) );
        assertEquals( neo4jLevel, config.get( middleware_logging_level ) );

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        var matcher = assertThat( logProvider ).forClass( Config.class ).forLevel( WARN );
        matcher.containsMessageWithArguments( "Use of deprecated setting %s. It is replaced by %s", oldSettingName, middleware_logging_level.name() );
        matcher.containsMessageWithArguments( "Old value format in %s used. %s migrated to %s", middleware_logging_level.name(), oldValue,
                neo4jLevel.toString() );
    }

    private static void testDisableMiddlewareLoggingMigration( String rawValue, Level configuredLevel, Level expectedLevel )
    {
        var settingName = "causal_clustering.disable_middleware_logging";
        Map<String, String> cfgMap = new HashMap<>();
        cfgMap.put( settingName, rawValue );
        cfgMap.put( middleware_logging_level.name(), configuredLevel.toString() );

        var config = Config.newBuilder().setRaw( cfgMap ).build();

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertThrows( IllegalArgumentException.class, () -> config.getSetting( settingName ) );
        assertEquals( expectedLevel, config.get( middleware_logging_level ) );

        if ( Objects.equals( TRUE, rawValue ) )
        {
            assertThat( logProvider ).forClass( Config.class ).forLevel( WARN ).containsMessageWithArguments(
                    "Use of deprecated setting %s. It is replaced by %s", settingName, middleware_logging_level.name() );
        }
    }

    private void testFailureWindows( String electionTimeoutSetting, DurationRange failureDetectionWindow, DurationRange failureResolutionWindow )
    {
        var setting = "causal_clustering.leader_election_timeout";
        var config = Config.newBuilder().setRaw( Map.of( setting, electionTimeoutSetting ) ).build();

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertEquals( failureDetectionWindow, config.get( failure_detection_window ) );
        assertEquals( failureResolutionWindow, config.get( failure_resolution_window ) );

        assertThat(logProvider).forClass( Config.class ).forLevel( WARN )
                               .containsMessageWithArguments( "Use of deprecated setting '%s'. It is replaced by '%s' and '%s'",
                                                              setting, failure_detection_window.name(), failure_resolution_window.name() );
    }
}
