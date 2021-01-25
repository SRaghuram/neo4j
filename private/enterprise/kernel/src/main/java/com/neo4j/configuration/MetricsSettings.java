/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.configuration.helpers.GlobbingPattern;
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
import static org.neo4j.configuration.SettingValueParsers.GLOBBING_PATTERN;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import static org.neo4j.configuration.SettingValueParsers.listOf;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;

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

    @Description( "Enable metrics. Setting this to `false` will to turn off all metrics." )
    public static final Setting<Boolean> metrics_enabled = newBuilder( "metrics.enabled", BOOL, true ).build();

    // The below settings define what metrics to gather
    @Description( "Specifies which metrics should be enabled by using a comma separated list of globbing patterns. " +
                  "Only the metrics matching the filter will be enabled. " +
                  "For example '*check_point*,neo4j.page_cache.evictions' will enable any checkpoint metrics and the pagecache eviction metric." )
    public static final Setting<List<GlobbingPattern>> metrics_filter = newBuilder( "metrics.filter", listOf( GLOBBING_PATTERN ),
            GlobbingPattern.create( "*bolt.connections*", "*bolt.messages_received*", "*bolt.messages_started*", "*dbms.pool.bolt.free",
                    "*dbms.pool.bolt.total_size", "*dbms.pool.bolt.total_used", "*dbms.pool.bolt.used_heap", "*causal_clustering.core.is_leader",
                    "*causal_clustering.core.last_leader_message", "*causal_clustering.core.replication_attempt", "*causal_clustering.core.replication_fail",
                    "*check_point.duration", "*check_point.total_time", "*cypher.replan_events", "*ids_in_use.node", "*ids_in_use.property",
                    "*ids_in_use.relationship", "*pool.transaction.*.total_used", "*pool.transaction.*.used_heap", "*pool.transaction.*.used_native",
                    "*store.size*", "*transaction.active_read", "*transaction.active_write", "*transaction.committed*", "*transaction.last_committed_tx_id",
                    "*transaction.peak_concurrent", "*transaction.rollbacks*", "*page_cache.hit*", "*page_cache.page_faults", "*page_cache.usage_ratio",
                    "*vm.file.descriptors.count", "*vm.gc.time.*", "*vm.heap.used", "*vm.memory.buffer.direct.used", "*vm.memory.pool.g1_eden_space",
                    "*vm.memory.pool.g1_old_gen", "*vm.pause_time", "*vm.thread*" ) ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics about transactions; number of transactions started, committed, etc. " +
                  "Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> neo_tx_enabled = newBuilder( "metrics.neo4j.tx.enabled", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics about the Neo4j page cache; page faults, evictions, flushes, exceptions, etc. " +
                  "Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> neo_page_cache_enabled = newBuilder( "metrics.neo4j.pagecache.enabled", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics about approximately how many entities are in the database; nodes, " +
                  "relationships, properties, etc. Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> neo_counts_enabled = newBuilder( "metrics.neo4j.counts.enabled", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics about the store size of each database. " +
                  "Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> neo_store_size_enabled = newBuilder( "metrics.neo4j.size.enabled", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics about number of entities in the database. " +
                  "Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> database_counts_enabled = newBuilder( "metrics.neo4j.data.counts.enabled", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics about Causal Clustering mode. " +
                  "Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> causal_clustering_enabled =
            newBuilder( "metrics.neo4j.causal_clustering.enabled", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics for Neo4j dbms operations; how many times databases have been created, started, stopped or dropped, " +
                  "and how many attempted operations have failed and recovered later. Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> database_operation_count_enabled =
            newBuilder( "metrics.neo4j.database_operation_count.enabled", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics about Neo4j check pointing; when it occurs and how much time it takes to " +
                  "complete. Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> neo_check_pointing_enabled =
            newBuilder( "metrics.neo4j.checkpointing.enabled", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics about the Neo4j transaction logs. " +
                  "Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> neo_transaction_logs_enabled = newBuilder( "metrics.neo4j.logs.enabled", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics about Server threading info. " +
                  "Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> neo_server_enabled = newBuilder( "metrics.neo4j.server.enabled", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics about the duration of garbage collections. " +
                  "Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> jvm_gc_enabled = newBuilder( "metrics.jvm.gc.enabled", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics about the heap memory usage. " +
                  "Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> jvm_heap_enabled = newBuilder( "metrics.jvm.heap.enabled", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics about the memory usage. " +
                  "Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> jvm_memory_enabled = newBuilder( "metrics.jvm.memory.enabled", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics about the buffer pools. " +
                  "Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> jvm_buffers_enabled = newBuilder( "metrics.jvm.buffers.enabled", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics about the current number of threads running. " +
                  "Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> jvm_threads_enabled = newBuilder( "metrics.jvm.threads.enabled", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics about the number of open file descriptors. " +
                  "Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> jvm_file_descriptors_enabled =
            newBuilder( "metrics.jvm.file.descriptors.enabled", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics about the VM pause time. " +
                  "Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> jvm_pause_time_enabled = newBuilder( "metrics.jvm.pause_time.enabled", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics about number of occurred replanning events. " +
                  "Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> cypher_planning_enabled = newBuilder( "metrics.cypher.replanning.enabled", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics about Bolt Protocol message processing. " +
                  "Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> bolt_messages_enabled = newBuilder( "metrics.bolt.messages.enabled", BOOL, false ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Enable reporting metrics about Neo4j memory pools. " +
                  "Deprecated - use metrics.filter instead." )
    public static final Setting<Boolean> neo_memory_pools_enabled = newBuilder( "metrics.neo4j.pools.enabled", BOOL, false ).build();

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
    public static final Setting<Duration> csv_interval = newBuilder( "metrics.csv.interval", DURATION, Duration.ofSeconds( 30 ) ).build();

    @Description( "The file size in bytes at which the csv files will auto-rotate. If set to zero then no " +
            "rotation will occur. Accepts a binary suffix `k`, `m` or `g`." )
    public static final Setting<Long> csv_rotation_threshold = newBuilder( "metrics.csv.rotation.size",
            BYTES, ByteUnit.mebiBytes( 10 ) ).addConstraint( range( 0L, Long.MAX_VALUE ) ).build();

    @Description( "Maximum number of history files for the csv files." )
    public static final Setting<Integer> csv_max_archives = newBuilder( "metrics.csv.rotation.keep_number", INT, 7 )
            .addConstraint( min( 1 ) )
            .build();

    @Description( "Decides what compression to use for the csv history files." )
    public static final Setting<CompressionOption> csv_archives_compression =
            newBuilder( "metrics.csv.rotation.compression", ofEnum( CompressionOption.class ), CompressionOption.NONE ).build();

    public enum CompressionOption
    {
        NONE,
        ZIP,
        GZ
    }

    // Graphite settings
    @Description( "Set to `true` to enable exporting metrics to Graphite." )
    public static final Setting<Boolean> graphite_enabled = newBuilder( "metrics.graphite.enabled", BOOL, false ).build();

    @Description( "The hostname or IP address of the Graphite server" )
    public static final Setting<SocketAddress> graphite_server = newBuilder( "metrics.graphite.server", SOCKET_ADDRESS, new SocketAddress( 2003 ) )
            .setDependency( default_listen_address )
            .build();

    @Description( "The reporting interval for Graphite. That is, how often to send updated metrics to Graphite." )
    public static final Setting<Duration> graphite_interval = newBuilder( "metrics.graphite.interval", DURATION, Duration.ofSeconds( 30 ) ).build();

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
