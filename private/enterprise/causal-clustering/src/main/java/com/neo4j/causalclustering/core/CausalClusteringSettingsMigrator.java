/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import org.apache.commons.lang3.math.NumberUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import org.neo4j.configuration.BaseConfigurationMigrator;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.logging.Level;

class CausalClusteringSettingsMigrator extends BaseConfigurationMigrator
{
    CausalClusteringSettingsMigrator()
    {
        add( new RoutingTtlMigrator() );
        add( new DisableMiddlewareLoggingMigrator() );
        add( new MiddlewareLoggingLevelMigrator() );
    }

    private static class RoutingTtlMigrator extends SpecificPropertyMigration
    {
        static final String OLD_SETTING = "causal_clustering.cluster_routing_ttl";
        static final String NEW_SETTING = GraphDatabaseSettings.routing_ttl.name();

        RoutingTtlMigrator()
        {
            super( OLD_SETTING, OLD_SETTING + " has been replaced with " + NEW_SETTING + "." );
        }

        @Override
        public void setValueWithOldSetting( String value, Map<String,String> rawConfiguration )
        {
            if ( value != null && !value.isEmpty() )
            {
                rawConfiguration.put( NEW_SETTING, value );
            }
        }
    }

    private static class DisableMiddlewareLoggingMigrator extends SpecificPropertyMigration
    {
        static final String SETTING_NAME = "causal_clustering.disable_middleware_logging";

        DisableMiddlewareLoggingMigrator()
        {
            super( SETTING_NAME, SETTING_NAME + " has been removed. Please configure the logging level instead using " +
                                 CausalClusteringSettings.middleware_logging_level.name() );
        }

        @Override
        public void setValueWithOldSetting( String value, Map<String,String> rawConfiguration )
        {
            if ( Objects.equals( Settings.TRUE, value ) )
            {
                rawConfiguration.put( CausalClusteringSettings.middleware_logging_level.name(), Level.NONE.toString() );
            }
        }
    }

    private static class MiddlewareLoggingLevelMigrator extends SpecificPropertyMigration
    {
        static final String SETTING_NAME = CausalClusteringSettings.middleware_logging_level.name();

        MiddlewareLoggingLevelMigrator()
        {
            super( SETTING_NAME, SETTING_NAME + " with integer value has been changed to use logging levels " + Arrays.toString( Level.values() ) );
        }

        @Override
        public boolean appliesTo( Map<String,String> rawConfiguration )
        {
            var stringValue = rawConfiguration.get( SETTING_NAME );
            if ( stringValue == null )
            {
                return false;
            }
            return NumberUtils.isParsable( stringValue ); // old value format is used when value can be parsed as an integer
        }

        @Override
        public void setValueWithOldSetting( String value, Map<String,String> rawConfiguration )
        {
            var level = parseLevel( value );
            rawConfiguration.put( SETTING_NAME, level.toString() );
        }

        static Level parseLevel( String value )
        {
            var levelInt = Integer.parseInt( value );
            if ( levelInt == java.util.logging.Level.OFF.intValue() )
            {
                return Level.NONE;
            }
            else if ( levelInt <= java.util.logging.Level.FINE.intValue() )
            {
                return Level.DEBUG;
            }
            else if ( levelInt <= java.util.logging.Level.INFO.intValue() )
            {
                return Level.INFO;
            }
            else if ( levelInt <= java.util.logging.Level.WARNING.intValue() )
            {
                return Level.WARN;
            }
            else
            {
                return Level.ERROR;
            }
        }
    }
}
