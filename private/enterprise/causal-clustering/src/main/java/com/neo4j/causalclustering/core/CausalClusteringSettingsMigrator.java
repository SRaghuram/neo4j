/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Map;
import java.util.Objects;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.SettingMigrator;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.middleware_logging_level;
import static org.neo4j.configuration.GraphDatabaseSettings.routing_ttl;
import static org.neo4j.configuration.SettingValueParsers.TRUE;

@ServiceProvider
public class CausalClusteringSettingsMigrator implements SettingMigrator
{
    @Override
    public void migrate( Map<String,String> input, Log log )
    {
        migrateRoutingTtl( input, log );
        migrateDisableMiddlewareLogging( input, log );
        migrateMiddlewareLoggingLevel( input, log );
    }

    private void migrateRoutingTtl( Map<String,String> input, Log log )
    {
        String oldSetting = "causal_clustering.cluster_routing_ttl";
        String newSetting = routing_ttl.name();
        String value = input.remove( oldSetting );
        if ( StringUtils.isNotEmpty( value ) )
        {
            log.warn( "Use of deprecated setting %s. It is replaced by %s", oldSetting, newSetting );
            input.putIfAbsent( newSetting, value );
        }
    }

    private void migrateDisableMiddlewareLogging( Map<String,String> input, Log log )
    {
        String oldSetting = "causal_clustering.disable_middleware_logging";
        String newSetting = middleware_logging_level.name();
        String value = input.remove( oldSetting );
        if ( Objects.equals( TRUE, value ) )
        {
            log.warn( "Use of deprecated setting %s. It is replaced by %s", oldSetting, newSetting );
            input.put( newSetting, Level.NONE.toString() );
        }
    }

    private void migrateMiddlewareLoggingLevel( Map<String,String> input, Log log )
    {
        String setting = middleware_logging_level.name();
        String value = input.get( setting );
        if ( StringUtils.isNotEmpty( value ) && NumberUtils.isParsable( value ) )
        {
            String level = parseLevel( value ).toString();
            log.warn( "Old value format in %s used. %s migrated to %s", setting, value, level );
            input.put( setting, level );
        }
    }

    private static Level parseLevel( String value )
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
