/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import org.apache.commons.lang3.math.NumberUtils;

import java.util.Map;
import java.util.Objects;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.SettingMigrator;
import org.neo4j.configuration.SettingMigrators;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.discovery_advertised_address;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.discovery_listen_address;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.middleware_logging_level;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_advertised_address;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_listen_address;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.transaction_advertised_address;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.transaction_listen_address;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.neo4j.configuration.GraphDatabaseSettings.routing_ttl;
import static org.neo4j.configuration.SettingMigrators.migrateAdvertisedAddressInheritanceChange;
import static org.neo4j.configuration.SettingValueParsers.TRUE;

@ServiceProvider
public class CausalClusteringSettingsMigrator implements SettingMigrator
{
    @Override
    public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
    {
        migrateRoutingTtl( values, log );
        migrateDisableMiddlewareLogging( values, log );
        migrateMiddlewareLoggingLevelSetting( values, log );
        migrateMiddlewareLoggingLevelValue( values, log );
        migrateAdvertisedAddresses( values, defaultValues, log );
    }

    private void migrateRoutingTtl( Map<String,String> values, Log log )
    {
        SettingMigrators.migrateSettingNameChange( values, log, "causal_clustering.cluster_routing_ttl", routing_ttl );
    }

    private void migrateDisableMiddlewareLogging( Map<String,String> values, Log log )
    {
        String oldSetting = "causal_clustering.disable_middleware_logging";
        String newSetting = middleware_logging_level.name();
        String value = values.remove( oldSetting );
        if ( Objects.equals( TRUE, value ) )
        {
            log.warn( "Use of deprecated setting %s. It is replaced by %s", oldSetting, newSetting );
            values.put( newSetting, Level.NONE.toString() );
        }
    }

    private void migrateMiddlewareLoggingLevelSetting( Map<String,String> input, Log log )
    {
        String oldSetting = "causal_clustering.middleware_logging.level";
        String newSetting = middleware_logging_level.name();
        String value = input.remove( oldSetting );
        if ( isNotBlank( value ) && NumberUtils.isParsable( value ) )
        {
            log.warn( "Use of deprecated setting %s. It is replaced by %s", oldSetting, newSetting );
            input.put( newSetting, value );
        }
    }

    private void migrateMiddlewareLoggingLevelValue( Map<String,String> values, Log log )
    {
        String setting = middleware_logging_level.name();
        String value = values.get( setting );
        if ( isNotBlank( value ) && NumberUtils.isParsable( value ) )
        {
            String level = parseLevel( value ).toString();
            log.warn( "Old value format in %s used. %s migrated to %s", setting, value, level );
            values.put( setting, level );
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

    private void migrateAdvertisedAddresses( Map<String,String> values, Map<String,String> defaultValues,  Log log )
    {
        migrateAdvertisedAddressInheritanceChange( values, defaultValues, log, transaction_listen_address.name(), transaction_advertised_address.name() );
        migrateAdvertisedAddressInheritanceChange( values, defaultValues, log, raft_listen_address.name(), raft_advertised_address.name() );
        migrateAdvertisedAddressInheritanceChange( values, defaultValues, log, discovery_listen_address.name(), discovery_advertised_address.name() );
    }
}
