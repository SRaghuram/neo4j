/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.configuration;

import java.nio.file.Path;
import java.time.Duration;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.io.ByteUnit;

import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingConstraints.range;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.HOSTNAME_PORT;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.configuration.SettingValueParsers.STRING;

@ServiceProvider
public class MetricsSettings implements SettingsDeclaration
{
    // Common settings
    @Description( "A common prefix for the reported metrics field names." )
    public static final Setting<String> metricsPrefix = newBuilder( "metrics.prefix", STRING, "neo4j" ).build();

    // The below settings define what metrics to gather
    // By default everything is on
    @Description( "Enable metrics. Setting this to `false` will to turn off all metrics." )
    public static final Setting<Boolean> metricsEnabled = newBuilder( "metrics.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about transactions; number of transactions started, committed, etc." )
    public static final Setting<Boolean> neoTxEnabled = newBuilder( "metrics.neo4j.tx.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about the Neo4j page cache; page faults, evictions, flushes, exceptions, " +
            "etc." )
    public static final Setting<Boolean> neoPageCacheEnabled = newBuilder( "metrics.neo4j.pagecache.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about approximately how many entities are in the database; nodes, " +
            "relationships, properties, etc." )
    public static final Setting<Boolean> neoCountsEnabled = newBuilder( "metrics.neo4j.counts.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about the store size of each database" )
    public static final Setting<Boolean> neoStoreSizeEnabled = newBuilder( "metrics.neo4j.size.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about number of entities in the database." )
    public static final Setting<Boolean> databaseCountsEnabled = newBuilder( "metrics.neo4j.data.counts.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about Causal Clustering mode." )
    public static final Setting<Boolean> causalClusteringEnabled =
            newBuilder( "metrics.neo4j.causal_clustering.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics for Neo4j dbms operations; " +
            "e.g. how many databases have been created or dropped, and how many attempted operations have failed." )
    public static final Setting<Boolean> databaseOperationCountEnabled =
            newBuilder( "metrics.neo4j.database_operation_count.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about Neo4j check pointing; when it occurs and how much time it takes to " +
            "complete." )
    public static final Setting<Boolean> neoCheckPointingEnabled =
            newBuilder( "metrics.neo4j.checkpointing.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about the Neo4j transaction logs" )
    public static final Setting<Boolean> neoTransactionLogsEnabled = newBuilder( "metrics.neo4j.logs.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about Server threading info." )
    public static final Setting<Boolean> neoServerEnabled = newBuilder( "metrics.neo4j.server.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about the duration of garbage collections" )
    public static final Setting<Boolean> jvmGcEnabled = newBuilder( "metrics.jvm.gc.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about the heap memory usage." )
    public static final Setting<Boolean> jvmHeapEnabled = newBuilder( "metrics.jvm.heap.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about the memory usage." )
    public static final Setting<Boolean> jvmMemoryEnabled = newBuilder( "metrics.jvm.memory.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about the buffer pools." )
    public static final Setting<Boolean> jvmBuffersEnabled = newBuilder( "metrics.jvm.buffers.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about the current number of threads running." )
    public static final Setting<Boolean> jvmThreadsEnabled = newBuilder( "metrics.jvm.threads.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about the number of open file descriptors." )
    public static final Setting<Boolean> jvmFileDescriptorsEnabled =
            newBuilder( "metrics.jvm.file.descriptors.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about number of occurred replanning events." )
    public static final Setting<Boolean> cypherPlanningEnabled = newBuilder( "metrics.cypher.replanning.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about Bolt Protocol message processing." )
    public static final Setting<Boolean> boltMessagesEnabled = newBuilder( "metrics.bolt.messages.enabled", BOOL, true ).build();

    @Description( "Enable reporting metrics about Neo4j memory pools." )
    public static final Setting<Boolean> neoMemoryPoolsEnabled = newBuilder( "metrics.neo4j.pools.enabled", BOOL, true ).build();

    // CSV settings
    @Description( "Set to `true` to enable exporting metrics to CSV files" )
    public static final Setting<Boolean> csvEnabled = newBuilder( "metrics.csv.enabled", BOOL, true ).build();

    @Description( "The target location of the CSV files: a path to a directory wherein a CSV file per reported " +
            "field  will be written." )
    public static final Setting<Path> csvPath = newBuilder( "dbms.directories.metrics", PATH, Path.of( "metrics" ) )
            .setDependency( neo4j_home )
            .immutable()
            .build();

    @Description( "The reporting interval for the CSV files. That is, how often new rows with numbers are appended to " +
            "the CSV files." )
    public static final Setting<Duration> csvInterval = newBuilder( "metrics.csv.interval", DURATION, Duration.ofSeconds( 3 ) ).build();

    @Description( "The file size in bytes at which the csv files will auto-rotate. If set to zero then no " +
            "rotation will occur. Accepts a binary suffix `k`, `m` or `g`." )
    public static final Setting<Long> csvRotationThreshold = newBuilder( "metrics.csv.rotation.size",
            BYTES, ByteUnit.mebiBytes( 10 ) ).addConstraint( range( 0L, Long.MAX_VALUE ) ).build();

    @Description( "Maximum number of history files for the csv files." )
    public static final Setting<Integer> csvMaxArchives = newBuilder( "metrics.csv.rotation.keep_number", INT, 7 )
            .addConstraint( min( 1 ) )
            .build();

    // Graphite settings
    @Description( "Set to `true` to enable exporting metrics to Graphite." )
    public static final Setting<Boolean> graphiteEnabled = newBuilder( "metrics.graphite.enabled", BOOL, false ).build();

    @Description( "The hostname or IP address of the Graphite server" )
    public static final Setting<HostnamePort> graphiteServer = newBuilder( "metrics.graphite.server", HOSTNAME_PORT, new HostnamePort( ":2003" ) ).build();

    @Description( "The reporting interval for Graphite. That is, how often to send updated metrics to Graphite." )
    public static final Setting<Duration> graphiteInterval = newBuilder( "metrics.graphite.interval", DURATION, Duration.ofSeconds( 3 ) ).build();

    // Prometheus settings
    @Description( "Set to `true` to enable the Prometheus endpoint" )
    public static final Setting<Boolean> prometheusEnabled = newBuilder( "metrics.prometheus.enabled", BOOL, false ).build();

    @Description( "The hostname and port to use as Prometheus endpoint" )
    public static final Setting<HostnamePort> prometheusEndpoint =
            newBuilder( "metrics.prometheus.endpoint", HOSTNAME_PORT, new HostnamePort( "localhost:2004" ) ).build();

    @Description( "Set to `true` to enable the JMX metrics endpoint" )
    public static final Setting<Boolean> jmxEnabled = newBuilder( "metrics.jmx.enabled", BOOL, true ).build();
}
