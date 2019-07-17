/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.neo4j.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Level;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.middleware_logging_level;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    private static void testMiddlewareLoggingLevelMigration( java.util.logging.Level julLevel, Level neo4jLevel )
    {
        var setting = middleware_logging_level;

        String oldValue = String.valueOf( julLevel.intValue() );
        var config = Config.defaults( setting, oldValue  );

        assertEquals( neo4jLevel, config.get( setting ) );

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        logProvider.assertAtLeastOnce( inLog( Config.class )
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
