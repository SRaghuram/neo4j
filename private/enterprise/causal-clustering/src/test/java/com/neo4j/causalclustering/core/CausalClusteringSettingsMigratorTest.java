/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Level;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.discovery_advertised_address;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.discovery_listen_address;
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
import static org.neo4j.logging.AssertableLogProvider.inLog;

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

        var config = Config.defaults( Map.of( setting.name(), Level.INFO.toString() ) );

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

    @TestFactory
    Collection<DynamicTest> testAddressMigration()
    {
        Collection<DynamicTest> tests = new ArrayList<>();
        tests.add( dynamicTest( "Test transaction address migration", () -> testAddrMigration( transaction_listen_address, transaction_advertised_address ) ) );
        tests.add( dynamicTest( "Test raft address migration", () -> testAddrMigration( raft_listen_address, raft_advertised_address ) ) );
        tests.add( dynamicTest( "Test discovery address migration", () -> testAddrMigration( discovery_listen_address, discovery_advertised_address ) ) );
        return tests;
    }

    private static void testAddrMigration( Setting<SocketAddress> listenAddr, Setting<SocketAddress> advertisedAddr )
    {
        Config config1 = Config.defaults( listenAddr, "foo:111" );
        Config config2 = Config.defaults( listenAddr, ":222" );
        Config config3 = Config.newBuilder().set( listenAddr, ":333" ).set( advertisedAddr, "bar" ).build();
        Config config4 = Config.newBuilder().set( listenAddr, "foo:444" ).set( advertisedAddr, ":555" ).build();
        Config config5 = Config.newBuilder().set( listenAddr, "foo" ).set( listenAddr, "bar" ).build();
        Config config6 = Config.newBuilder().set( listenAddr, "foo:666" ).set( advertisedAddr, "bar:777" ).build();

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
        assertEquals( new SocketAddress( "bar", advertisedAddr.defaultValue().getPort() ), config5.get( listenAddr ) );
        assertEquals( new SocketAddress( "bar", 777 ), config6.get( advertisedAddr ) );

        String msg = "Use of deprecated setting port propagation. port %s is migrated from %s to %s.";

        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( msg, 111, listenAddr.name(), advertisedAddr.name() ) );
        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( msg, 222, listenAddr.name(), advertisedAddr.name() ) );
        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( msg, 333, listenAddr.name(), advertisedAddr.name() ) );

        logProvider.assertNone( inLog( Config.class ).warn( msg, 444, listenAddr.name(), advertisedAddr.name() ) );
        logProvider.assertNone( inLog( Config.class ).warn( msg, 555, listenAddr.name(), advertisedAddr.name() ) );
        logProvider.assertNone( inLog( Config.class ).warn( msg, 666, listenAddr.name(), advertisedAddr.name() ) );
    }

    private static void testRoutingTtlSettingMigration( String rawValue, Duration expectedValue )
    {
        String setting = "causal_clustering.cluster_routing_ttl";
        var config = Config.defaults( Map.of( setting, rawValue ) );

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertThrows( IllegalArgumentException.class, () -> config.getSetting( setting ) );
        assertEquals( expectedValue, config.get( routing_ttl ) );

        if ( StringUtils.isNotEmpty( rawValue ) )
        {
            logProvider.assertAtLeastOnce(
                    inLog( Config.class ).warn( "Use of deprecated setting %s. It is replaced by %s", setting, routing_ttl.name() ) );
        }
    }

    private static void testMiddlewareLoggingLevelMigrationFromNewSetting( java.util.logging.Level julLevel, Level neo4jLevel )
    {
        var setting = middleware_logging_level;
        var oldValue = String.valueOf( julLevel.intValue() );
        var config = Config.defaults( setting, oldValue );

        assertEquals( neo4jLevel, config.get( setting ) );

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        logProvider.assertAtLeastOnce(
                inLog( Config.class )
                        .warn( "Old value format in %s used. %s migrated to %s", middleware_logging_level.name(), oldValue, neo4jLevel.toString() ) );
    }

    private static void testMiddlewareLoggingLevelMigrationFromOldSetting( java.util.logging.Level julLevel, Level neo4jLevel )
    {
        var oldSettingName = "causal_clustering.middleware_logging.level";
        var oldValue = String.valueOf( julLevel.intValue() );
        var config = Config.defaults( MapUtil.genericMap( oldSettingName, oldValue ) );

        assertThrows( IllegalArgumentException.class, () -> config.getSetting( oldSettingName ) );
        assertEquals( neo4jLevel, config.get( middleware_logging_level ) );

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        logProvider.assertAtLeastOnce(
                inLog( Config.class )
                        .warn( "Use of deprecated setting %s. It is replaced by %s", oldSettingName, middleware_logging_level.name() ),
                inLog( Config.class )
                        .warn( "Old value format in %s used. %s migrated to %s", middleware_logging_level.name(), oldValue, neo4jLevel.toString() ) );
    }

    private static void testDisableMiddlewareLoggingMigration( String rawValue, Level configuredLevel, Level expectedLevel )
    {
        var settingName = "causal_clustering.disable_middleware_logging";
        Map<String, String> cfgMap = new HashMap<>();
        cfgMap.put( settingName, rawValue );
        cfgMap.put( middleware_logging_level.name(), configuredLevel.toString() );

        var config = Config.defaults( cfgMap );

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertThrows( IllegalArgumentException.class, () -> config.getSetting( settingName ) );
        assertEquals( expectedLevel, config.get( middleware_logging_level ) );

        if ( Objects.equals( TRUE, rawValue ) )
        {
            logProvider.assertAtLeastOnce( inLog( Config.class )
                    .warn( "Use of deprecated setting %s. It is replaced by %s", settingName, middleware_logging_level.name() ) );
        }
    }

}
