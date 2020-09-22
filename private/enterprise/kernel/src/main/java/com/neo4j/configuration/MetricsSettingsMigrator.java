/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.SettingMigrator;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.configuration.helpers.GlobbingPattern;
import org.neo4j.logging.Log;

import static com.neo4j.configuration.MetricsSettings.bolt_messages_enabled;
import static com.neo4j.configuration.MetricsSettings.causal_clustering_enabled;
import static com.neo4j.configuration.MetricsSettings.cypher_planning_enabled;
import static com.neo4j.configuration.MetricsSettings.database_counts_enabled;
import static com.neo4j.configuration.MetricsSettings.database_operation_count_enabled;
import static com.neo4j.configuration.MetricsSettings.jvm_buffers_enabled;
import static com.neo4j.configuration.MetricsSettings.jvm_file_descriptors_enabled;
import static com.neo4j.configuration.MetricsSettings.jvm_gc_enabled;
import static com.neo4j.configuration.MetricsSettings.jvm_heap_enabled;
import static com.neo4j.configuration.MetricsSettings.jvm_memory_enabled;
import static com.neo4j.configuration.MetricsSettings.jvm_pause_time_enabled;
import static com.neo4j.configuration.MetricsSettings.jvm_threads_enabled;
import static com.neo4j.configuration.MetricsSettings.metrics_filter;
import static com.neo4j.configuration.MetricsSettings.metrics_namespaces_enabled;
import static com.neo4j.configuration.MetricsSettings.metrics_prefix;
import static com.neo4j.configuration.MetricsSettings.neo_check_pointing_enabled;
import static com.neo4j.configuration.MetricsSettings.neo_counts_enabled;
import static com.neo4j.configuration.MetricsSettings.neo_memory_pools_enabled;
import static com.neo4j.configuration.MetricsSettings.neo_page_cache_enabled;
import static com.neo4j.configuration.MetricsSettings.neo_server_enabled;
import static com.neo4j.configuration.MetricsSettings.neo_store_size_enabled;
import static com.neo4j.configuration.MetricsSettings.neo_transaction_logs_enabled;
import static com.neo4j.configuration.MetricsSettings.neo_tx_enabled;
import static java.util.Collections.emptyList;

@ServiceProvider
public class MetricsSettingsMigrator implements SettingMigrator
{
    @Override
    public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
    {
        migrateEnabledMetricsToMetricsFilter( values, log );
    }

    private static void addEnabledLegacyMetricSettingToFilter( Map<String,String> values, Log log, List<String> metricsFilter,
            String oldSetting, MetricNamesPrefixBuilder prefixBuilder, List<String> databaseMetrics, List<String> globalMetrics )
    {
        String value = values.remove( oldSetting );
        if ( SettingValueParsers.TRUE.equalsIgnoreCase( value ) )
        {
            log.warn( "Use of deprecated setting %s. It is replaced by %s", oldSetting, MetricsSettings.metrics_filter.name() );
            for ( String databaseMetric : databaseMetrics )
            {
                metricsFilter.add( prefixBuilder.prefixDatabaseMetricName( databaseMetric ) );
            }
            for ( String globalMetric : globalMetrics )
            {
                metricsFilter.add( prefixBuilder.prefixGlobalMetricName( globalMetric ) );
            }
        }
    }

    private static void migrateEnabledMetricsToMetricsFilter( Map<String,String> values, Log log )
    {
        MetricNamesPrefixBuilder prefixBuilder = new MetricNamesPrefixBuilder( values );
        List<String> amendedMetricsFilter = new ArrayList<>();

        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, neo_tx_enabled.name(), prefixBuilder,
                List.of( "transaction.*" ), emptyList() );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, neo_page_cache_enabled.name(), prefixBuilder,
                emptyList(), List.of( "page_cache.*" ) );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, neo_counts_enabled.name(), prefixBuilder,
                List.of( "ids_in_use.*" ), emptyList() );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, neo_store_size_enabled.name(), prefixBuilder,
                List.of( "store.size.*" ), emptyList() );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, database_counts_enabled.name(), prefixBuilder,
                List.of( "neo4j.count.*" ), emptyList() );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, causal_clustering_enabled.name(), prefixBuilder,
                List.of( "causal_clustering.core.*", "causal_clustering.catchup.*", "causal_clustering.read_replica.*" ),
                List.of( "causal_clustering.core.discovery.*" ) );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, database_operation_count_enabled.name(), prefixBuilder,
                emptyList(), List.of( "db.operation.count.*" ) );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, neo_check_pointing_enabled.name(), prefixBuilder,
                List.of( "check_point.*" ), emptyList() );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, neo_transaction_logs_enabled.name(), prefixBuilder,
                List.of( "log.*" ), emptyList() );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, "metrics.neo4j.logrotation.enabled", prefixBuilder,
                List.of( "log.*" ), emptyList() );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, neo_server_enabled.name(), prefixBuilder,
                emptyList(), List.of( "server.*" ) );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, jvm_gc_enabled.name(), prefixBuilder,
                emptyList(), List.of( "vm.gc.*" ) );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, jvm_heap_enabled.name(), prefixBuilder,
                emptyList(), List.of( "vm.heap.*" ) );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, jvm_memory_enabled.name(), prefixBuilder,
                emptyList(), List.of( "vm.memory.pool.*" ) );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, jvm_buffers_enabled.name(), prefixBuilder,
                emptyList(), List.of( "vm.memory.buffer.*" ) );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, jvm_threads_enabled.name(), prefixBuilder,
                emptyList(), List.of( "vm.thread.*" ) );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, jvm_file_descriptors_enabled.name(), prefixBuilder,
                emptyList(), List.of( "vm.file.descriptors.*" ) );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, jvm_pause_time_enabled.name(), prefixBuilder,
                emptyList(), List.of( "vm.pause_time.*" ) );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, cypher_planning_enabled.name(), prefixBuilder,
                List.of( "cypher.*" ), emptyList() );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, bolt_messages_enabled.name(), prefixBuilder,
                emptyList(), List.of( "bolt.*" ) );
        addEnabledLegacyMetricSettingToFilter( values, log, amendedMetricsFilter, neo_memory_pools_enabled.name(), prefixBuilder,
                List.of( "pool.*" ), List.of( "pool.*", "dbms.pool.*" ) );

        if ( amendedMetricsFilter.size() != 0 )
        {
            String metricsFilter = values.get( metrics_filter.name() );
            if ( metricsFilter == null )
            {
                for ( GlobbingPattern globbingPattern : metrics_filter.defaultValue() )
                {
                    amendedMetricsFilter.add( globbingPattern.toString() );
                }
            }
            else if ( !metricsFilter.isEmpty() )
            {
                amendedMetricsFilter.add( metricsFilter );
            }
            values.put( metrics_filter.name(), String.join( ",", amendedMetricsFilter ) );
        }
    }

    private static class MetricNamesPrefixBuilder
    {
        private final String globalMetricsPrefix;
        private final String databaseMetricsPrefix;

        MetricNamesPrefixBuilder( Map<String,String> values )
        {
            String metricsPrefix = values.getOrDefault( metrics_prefix.name(), metrics_prefix.defaultValue() );
            StringBuilder globalMetricsPrefix = new StringBuilder( metricsPrefix );
            StringBuilder databaseMetricsPrefix = new StringBuilder( metricsPrefix );
            globalMetricsPrefix.append( '.' );
            databaseMetricsPrefix.append( '.' );
            if ( namespacesEnabled( values ) )
            {
                globalMetricsPrefix.append( "dbms." );
                databaseMetricsPrefix.append( "database." );
            }
            databaseMetricsPrefix.append( "*." );
            this.globalMetricsPrefix = globalMetricsPrefix.toString();
            this.databaseMetricsPrefix = databaseMetricsPrefix.toString();
        }

        String prefixGlobalMetricName( String metricName )
        {
            return globalMetricsPrefix + metricName;
        }

        String prefixDatabaseMetricName( String metricName )
        {
            return databaseMetricsPrefix + metricName;
        }

        private boolean namespacesEnabled( Map<String,String> values )
        {
            String namespacesEnabledString = values.get( metrics_namespaces_enabled.name() );
            boolean namespacesEnabledBool = metrics_namespaces_enabled.defaultValue();

            if ( namespacesEnabledString != null )
            {
                namespacesEnabledBool = SettingValueParsers.BOOL.parse( namespacesEnabledString );
            }
            return namespacesEnabledBool;
        }
    }
}
