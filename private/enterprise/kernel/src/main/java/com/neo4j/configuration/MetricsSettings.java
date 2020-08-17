/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import java.nio.file.Path;
import java.time.Duration;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;

import static org.neo4j.configuration.GraphDatabaseSettings.default_listen_address;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingConstraints.range;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS;
import static org.neo4j.configuration.SettingValueParsers.STRING;

@ServiceProvider
@PublicApi
public class MetricsSettings implements SettingsDeclaration
{
    // Common settings
    @Description( "A common prefix for the reported metrics field names." )
    public static final Setting<String> metrics_prefix = newBuilder( "metrics.prefix", STRING, "neo4j" ).build();

    @Description( "Enable metrics namespaces that separates the global and database specific metrics. " +
                  "If enabled all database specific metrics will have field names starting with <metrics_prefix>.database.<database_name> " +
                  "and all global metrics will start with <metrics_prefix>.dbms. " +
                  "For example neo4j.page_cache.hits will become neo4j.dbms.page_cache.hits and " +
                  "neo4j.system.log.rotation_events will become neo4j.database.system.log.rotation_events." )
    public static final Setting<Boolean> metrics_namespaces_enabled = newBuilder( "metrics.namespaces.enabled", BOOL, false ).build();

    // The below settings define what metrics to gather
    // By default everything is on
    @Description( "Enable metrics. Setting this to `false` will to turn off all metrics." )
    public static final Setting<Boolean> metrics_enabled = newBuilder( "metrics.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about transactions; number of transactions started, committed, etc." )
    public static final Setting<Boolean> neo_tx_enabled = newBuilder( "metrics.neo4j.tx.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about the Neo4j page cache; page faults, evictions, flushes, exceptions, etc." )
    public static final Setting<Boolean> neo_page_cache_enabled = newBuilder( "metrics.neo4j.pagecache.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about approximately how many entities are in the database; nodes, " +
            "relationships, properties, etc." )
    public static final Setting<Boolean> neo_counts_enabled = newBuilder( "metrics.neo4j.counts.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about the store size of each database" )
    public static final Setting<Boolean> neo_store_size_enabled = newBuilder( "metrics.neo4j.size.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about number of entities in the database." )
    public static final Setting<Boolean> database_counts_enabled = newBuilder( "metrics.neo4j.data.counts.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about Causal Clustering mode." )
    public static final Setting<Boolean> causal_clustering_enabled =
            newBuilder( "metrics.neo4j.causal_clustering.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics for Neo4j dbms operations; " +
            "how many times databases have been created, started, stopped or dropped, and how many attempted operations have failed and recovered later." )
    public static final Setting<Boolean> database_operation_count_enabled =
            newBuilder( "metrics.neo4j.database_operation_count.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about Neo4j check pointing; when it occurs and how much time it takes to " +
            "complete." )
    public static final Setting<Boolean> neo_check_pointing_enabled =
            newBuilder( "metrics.neo4j.checkpointing.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about the Neo4j transaction logs" )
    public static final Setting<Boolean> neo_transaction_logs_enabled = newBuilder( "metrics.neo4j.logs.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about Server threading info." )
    public static final Setting<Boolean> neo_server_enabled = newBuilder( "metrics.neo4j.server.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about the duration of garbage collections" )
    public static final Setting<Boolean> jvm_gc_enabled = newBuilder( "metrics.jvm.gc.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about the heap memory usage." )
    public static final Setting<Boolean> jvm_heap_enabled = newBuilder( "metrics.jvm.heap.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about the memory usage." )
    public static final Setting<Boolean> jvm_memory_enabled = newBuilder( "metrics.jvm.memory.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about the buffer pools." )
    public static final Setting<Boolean> jvm_buffers_enabled = newBuilder( "metrics.jvm.buffers.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about the current number of threads running." )
    public static final Setting<Boolean> jvm_threads_enabled = newBuilder( "metrics.jvm.threads.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about the number of open file descriptors." )
    public static final Setting<Boolean> jvm_file_descriptors_enabled =
            newBuilder( "metrics.jvm.file.descriptors.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about the VM pause time." )
    public static final Setting<Boolean> jvm_pause_time_enabled = newBuilder( "metrics.jvm.pause_time.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about number of occurred replanning events." )
    public static final Setting<Boolean> cypher_planning_enabled = newBuilder( "metrics.cypher.replanning.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about Bolt Protocol message processing." )
    public static final Setting<Boolean> bolt_messages_enabled = newBuilder( "metrics.bolt.messages.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about Neo4j memory pools." )
    public static final Setting<Boolean> neo_memory_pools_enabled = newBuilder( "metrics.neo4j.pools.enabled", BOOL, true ).build();

    // CSV settings
    @Description( "Set to `true` to enable exporting metrics to CSV files" )
    public static final Setting<Boolean> csv_enabled = newBuilder( "metrics.csv.enabled", BOOL, true ).build();

    @Description( "The target location of the CSV files: a path to a directory wherein a CSV file per reported " +
            "field  will be written." )
    public static final Setting<Path> csv_path = newBuilder( "dbms.directories.metrics", PATH, Path.of( "metrics" ) )
            .setDependency( neo4j_home )
            .immutable()
            .build();

    @Description( "The reporting interval for the CSV files. That is, how often new rows with numbers are appended to " +
            "the CSV files." )
    public static final Setting<Duration> csv_interval = newBuilder( "metrics.csv.interval", DURATION, Duration.ofSeconds( 3 ) ).build();

    @Description( "The file size in bytes at which the csv files will auto-rotate. If set to zero then no " +
            "rotation will occur. Accepts a binary suffix `k`, `m` or `g`." )
    public static final Setting<Long> csv_rotation_threshold = newBuilder( "metrics.csv.rotation.size",
            BYTES, ByteUnit.mebiBytes( 10 ) ).addConstraint( range( 0L, Long.MAX_VALUE ) ).build();

    @Description( "Maximum number of history files for the csv files." )
    public static final Setting<Integer> csv_max_archives = newBuilder( "metrics.csv.rotation.keep_number", INT, 7 )
            .addConstraint( min( 1 ) )
            .build();

    // Graphite settings
    @Description( "Set to `true` to enable exporting metrics to Graphite." )
    public static final Setting<Boolean> graphite_enabled = newBuilder( "metrics.graphite.enabled", BOOL, false ).build();

    @Description( "The hostname or IP address of the Graphite server" )
    public static final Setting<SocketAddress> graphite_server = newBuilder( "metrics.graphite.server", SOCKET_ADDRESS, new SocketAddress( 2003 ) )
            .setDependency( default_listen_address )
            .build();

    @Description( "The reporting interval for Graphite. That is, how often to send updated metrics to Graphite." )
    public static final Setting<Duration> graphite_interval = newBuilder( "metrics.graphite.interval", DURATION, Duration.ofSeconds( 3 ) ).build();

    // Prometheus settings
    @Description( "Set to `true` to enable the Prometheus endpoint" )
    public static final Setting<Boolean> prometheus_enabled = newBuilder( "metrics.prometheus.enabled", BOOL, false ).build();

    @Description( "The hostname and port to use as Prometheus endpoint" )
    public static final Setting<SocketAddress> prometheus_endpoint =
            newBuilder( "metrics.prometheus.endpoint", SOCKET_ADDRESS, new SocketAddress( "localhost", 2004 ) )
                    .setDependency( default_listen_address ).build();

    @Description( "Set to `true` to enable the JMX metrics endpoint" )
    public static final Setting<Boolean> jmx_enabled = newBuilder( "metrics.jmx.enabled", BOOL, true ).build();
}
