/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import org.apache.commons.lang3.math.NumberUtils;

import java.util.Map;
import java.util.Objects;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.SettingMigrator;
import org.neo4j.configuration.helpers.DurationRange;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;

import static com.neo4j.configuration.CausalClusteringInternalSettings.akka_bind_timeout;
import static com.neo4j.configuration.CausalClusteringInternalSettings.akka_connection_timeout;
import static com.neo4j.configuration.CausalClusteringInternalSettings.akka_handshake_timeout;
import static com.neo4j.configuration.CausalClusteringInternalSettings.middleware_akka_default_parallelism_level;
import static com.neo4j.configuration.CausalClusteringInternalSettings.middleware_akka_sink_parallelism_level;
import static com.neo4j.configuration.CausalClusteringSettings.connect_randomly_to_server_group_strategy;
import static com.neo4j.configuration.CausalClusteringSettings.discovery_advertised_address;
import static com.neo4j.configuration.CausalClusteringSettings.discovery_listen_address;
import static com.neo4j.configuration.CausalClusteringSettings.election_failure_detection_window;
import static com.neo4j.configuration.CausalClusteringSettings.leader_failure_detection_window;
import static com.neo4j.configuration.CausalClusteringSettings.middleware_logging_level;
import static com.neo4j.configuration.CausalClusteringSettings.raft_advertised_address;
import static com.neo4j.configuration.CausalClusteringSettings.raft_listen_address;
import static com.neo4j.configuration.CausalClusteringSettings.transaction_advertised_address;
import static com.neo4j.configuration.CausalClusteringSettings.transaction_listen_address;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.neo4j.configuration.GraphDatabaseSettings.routing_ttl;
import static org.neo4j.configuration.SettingMigrators.migrateAdvertisedAddressInheritanceChange;
import static org.neo4j.configuration.SettingMigrators.migrateSettingNameChange;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.DURATION_RANGE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;

@ServiceProvider
public class CausalClusteringSettingsMigrator implements SettingMigrator
{
    /**
     * This is the default window size for the failure detection window in case 'causal_clustering.leader_election_timeout' would be still defined.
     * 5 seconds must be enough to cause enough variance in the failure detection timer through the members of a cluster.
     */
    static final int DEFAULT_FAILURE_DETECTION_MAX_WINDOW_IN_SECONDS = 5;

    @Override
    public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
    {
        migrateRoutingTtl( values, log );
        migrateDisableMiddlewareLogging( values, log );
        migrateMiddlewareLoggingLevelSetting( values, log );
        migrateMiddlewareLoggingLevelValue( values, log );
        migrateAdvertisedAddresses( values, defaultValues, log );
        migrateElectionTimeout( values, defaultValues, log );
        migrateConnectRandomly( values, log );
        migrateMiddlewareAkka( values, log );
    }

    private void migrateRoutingTtl( Map<String,String> values, Log log )
    {
        migrateSettingNameChange( values, log, "causal_clustering.cluster_routing_ttl", routing_ttl );
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

    private void migrateElectionTimeout( Map<String,String> values, Map<String,String> defaultValues, Log log )
    {
        var leaderElectionTimoutSetting = "causal_clustering.leader_election_timeout";
        var failureDetectionWindowSetting = leader_failure_detection_window.name();
        var failureResolutionWindowSetting = election_failure_detection_window.name();
        var leaderElectionTimoutValue = values.get( leaderElectionTimoutSetting );
        var failureDetectionWindowValue = values.get( failureDetectionWindowSetting );
        var failureResolutionWindowValue = values.get( failureResolutionWindowSetting );
        if ( isNotBlank( leaderElectionTimoutValue ) )
        {
            if ( isNotBlank( failureDetectionWindowValue ) )
            {
                log.warn( "Deprecated setting '%s' is ignored because replacement '%s' and '%s' is set",
                          leaderElectionTimoutSetting, failureDetectionWindowSetting, failureResolutionWindowSetting );
            }
            else
            {
                convertElectionTimeout( values, defaultValues, leaderElectionTimoutValue, failureResolutionWindowValue, log );
            }
        }
    }

    private void convertElectionTimeout( Map<String,String> values, Map<String,String> defaultValues,
                                         String leaderElectionTimoutValue, String failureResolutionWindowValue, Log log )
    {
        var leaderElectionTimoutSetting = "causal_clustering.leader_election_timeout";
        var failureDetectionWindowSetting = leader_failure_detection_window.name();
        var failureResolutionWindowSetting = election_failure_detection_window.name();

        log.warn( "Use of deprecated setting '%s'. It is replaced by '%s' and '%s'",
                  leaderElectionTimoutSetting, failureDetectionWindowSetting, failureResolutionWindowSetting );

        var min = DURATION.parse( leaderElectionTimoutValue );
        // the window is the smaller of this two: DEFAULT_FAILURE_DETECTION_MAX_WINDOW_IN_SECONDS or the original electionTimeout
        var max = min.plusMillis( Math.min( min.toMillis(), DEFAULT_FAILURE_DETECTION_MAX_WINDOW_IN_SECONDS * 1000 ) );
        var failureDetectionWindowValue = new DurationRange( min, max ).valueToString();
        values.put( failureDetectionWindowSetting, failureDetectionWindowValue );

        if ( isBlank( failureResolutionWindowValue ) )
        {
            var defaultFailureResolutionWindow = election_failure_detection_window.defaultValue();
            var overriddenFailureResolutionWindowValue = defaultValues.get( failureResolutionWindowSetting );
            if ( isNotBlank( overriddenFailureResolutionWindowValue ) )
            {
                defaultFailureResolutionWindow = DURATION_RANGE.parse( overriddenFailureResolutionWindowValue );
            }
            // if old setting is shorter than default resolution window minimum then resolution window is set identical to detection window
            if ( min.toMillis() < defaultFailureResolutionWindow.getMin().toMillis() )
            {
                values.put( failureResolutionWindowSetting,failureDetectionWindowValue );
            }
        }
    }

    private void migrateConnectRandomly( Map<String,String> values, Log log )
    {
        migrateSettingNameChange( values, log, "causal_clustering.connect-randomly-to-server-group", connect_randomly_to_server_group_strategy );
    }

    private void migrateMiddlewareAkka( Map<String,String> values, Log log )
    {
        migrateSettingNameChange( values, log, "causal_clustering.middleware.akka.default-parallelism", middleware_akka_default_parallelism_level );
        migrateSettingNameChange( values, log, "causal_clustering.middleware.akka.sink-parallelism", middleware_akka_sink_parallelism_level );
        migrateSettingNameChange( values, log, "causal_clustering.middleware.akka.bind-timeout", akka_bind_timeout );
        migrateSettingNameChange( values, log, "causal_clustering.middleware.akka.connection-timeout", akka_connection_timeout );
        migrateSettingNameChange( values, log, "causal_clustering.middleware.akka.handshake-timeout", akka_handshake_timeout );
    }
}
