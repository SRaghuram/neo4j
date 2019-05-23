/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics;

import java.io.File;
import java.time.Duration;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;

import static org.neo4j.configuration.Settings.BOOLEAN;
import static org.neo4j.configuration.Settings.BYTES;
import static org.neo4j.configuration.Settings.DURATION;
import static org.neo4j.configuration.Settings.FALSE;
import static org.neo4j.configuration.Settings.HOSTNAME_PORT;
import static org.neo4j.configuration.Settings.INTEGER;
import static org.neo4j.configuration.Settings.STRING;
import static org.neo4j.configuration.Settings.TRUE;
import static org.neo4j.configuration.Settings.buildSetting;
import static org.neo4j.configuration.Settings.min;
import static org.neo4j.configuration.Settings.pathSetting;
import static org.neo4j.configuration.Settings.range;
import static org.neo4j.configuration.Settings.setting;

/**
 * Settings for the Neo4j Enterprise metrics reporting.
 */
@Description( "Metrics settings" )
@ServiceProvider
public class MetricsSettings implements LoadableConfig
{
    // Common settings
    @Description( "A common prefix for the reported metrics field names." )
    public static final Setting<String> metricsPrefix = setting( "metrics.prefix", STRING, "neo4j" );

    // The below settings define what metrics to gather
    // By default everything is on
    @Description( "Enable metrics. Setting this to `false` will to turn off all metrics." )
    public static final Setting<Boolean> metricsEnabled = setting( "metrics.enabled", BOOLEAN, TRUE );

    @Description( "Enable reporting metrics about transactions; number of transactions started, committed, etc." )
    public static final Setting<Boolean> neoTxEnabled = setting( "metrics.neo4j.tx.enabled", BOOLEAN, TRUE );

    @Description( "Enable reporting metrics about the Neo4j page cache; page faults, evictions, flushes, exceptions, " +
                  "etc." )
    public static final Setting<Boolean> neoPageCacheEnabled = setting( "metrics.neo4j.pagecache.enabled", BOOLEAN, TRUE );

    @Description( "Enable reporting metrics about approximately how many entities are in the database; nodes, " +
                  "relationships, properties, etc." )
    public static final Setting<Boolean> neoCountsEnabled = setting( "metrics.neo4j.counts.enabled", BOOLEAN, TRUE );

    @Description( "Enable reporting metrics about Causal Clustering mode." )
    public static final Setting<Boolean> causalClusteringEnabled = setting( "metrics.neo4j.causal_clustering.enabled", BOOLEAN, TRUE );

    @Description( "Enable reporting metrics about Neo4j check pointing; when it occurs and how much time it takes to " +
                  "complete." )
    public static final Setting<Boolean> neoCheckPointingEnabled = setting( "metrics.neo4j.checkpointing.enabled", BOOLEAN, TRUE );

    @Description( "Enable reporting metrics about the Neo4j log rotation; when it occurs and how much time it takes to "
                  + "complete." )
    public static final Setting<Boolean> neoLogRotationEnabled = setting( "metrics.neo4j.logrotation.enabled", BOOLEAN, TRUE );

    @Description( "Enable reporting metrics about Server threading info." )
    public static final Setting<Boolean> neoServerEnabled = setting( "metrics.neo4j.server.enabled", BOOLEAN, TRUE );

    @Description( "Enable reporting metrics about the duration of garbage collections" )
    public static final Setting<Boolean> jvmGcEnabled = setting( "metrics.jvm.gc.enabled", BOOLEAN, TRUE );

    @Description( "Enable reporting metrics about the memory usage." )
    public static final Setting<Boolean> jvmMemoryEnabled = setting( "metrics.jvm.memory.enabled", BOOLEAN, TRUE );

    @Description( "Enable reporting metrics about the buffer pools." )
    public static final Setting<Boolean> jvmBuffersEnabled = setting( "metrics.jvm.buffers.enabled", BOOLEAN, TRUE );

    @Description( "Enable reporting metrics about the current number of threads running." )
    public static final Setting<Boolean> jvmThreadsEnabled = setting( "metrics.jvm.threads.enabled", BOOLEAN, TRUE );

    @Description( "Enable reporting metrics about the number of open file descriptors." )
    public static final Setting<Boolean> jvmFileDescriptorsEnabled = setting( "metrics.jvm.file.descriptors.enabled", BOOLEAN, TRUE );

    @Description( "Enable reporting metrics about number of occurred replanning events." )
    public static final Setting<Boolean> cypherPlanningEnabled = setting( "metrics.cypher.replanning.enabled", BOOLEAN, TRUE );

    @Description( "Enable reporting metrics about Bolt Protocol message processing." )
    public static final Setting<Boolean> boltMessagesEnabled = setting( "metrics.bolt.messages.enabled", BOOLEAN, TRUE );

    // CSV settings
    @Description( "Set to `true` to enable exporting metrics to CSV files" )
    public static final Setting<Boolean> csvEnabled = setting( "metrics.csv.enabled", BOOLEAN, TRUE );

    @Description( "The target location of the CSV files: a path to a directory wherein a CSV file per reported " +
                  "field  will be written." )
    public static final Setting<File> csvPath = pathSetting( "dbms.directories.metrics", "metrics" );

    @Description( "The reporting interval for the CSV files. That is, how often new rows with numbers are appended to " +
                  "the CSV files." )
    public static final Setting<Duration> csvInterval = setting( "metrics.csv.interval", DURATION, "3s" );

    @Description( "The file size in bytes at which the csv files will auto-rotate. If set to zero then no " +
            "rotation will occur. Accepts a binary suffix `k`, `m` or `g`." )
    public static final Setting<Long> csvRotationThreshold = buildSetting( "metrics.csv.rotation.size",
            BYTES, "10m" ).constraint( range( 0L, Long.MAX_VALUE ) ).build();

    @Description( "Maximum number of history files for the csv files." )
    public static final Setting<Integer> csvMaxArchives = buildSetting( "metrics.csv.rotation.keep_number",
            INTEGER, "7" ).constraint( min( 1 ) ).build();

    // Graphite settings
    @Description( "Set to `true` to enable exporting metrics to Graphite." )
    public static final Setting<Boolean> graphiteEnabled = setting( "metrics.graphite.enabled", BOOLEAN, FALSE );

    @Description( "The hostname or IP address of the Graphite server" )
    public static final Setting<HostnamePort> graphiteServer = setting( "metrics.graphite.server", HOSTNAME_PORT, ":2003" );

    @Description( "The reporting interval for Graphite. That is, how often to send updated metrics to Graphite." )
    public static final Setting<Duration> graphiteInterval = setting( "metrics.graphite.interval", DURATION, "3s" );

    // Prometheus settings
    @Description( "Set to `true` to enable the Prometheus endpoint" )
    public static final Setting<Boolean> prometheusEnabled = setting( "metrics.prometheus.enabled", BOOLEAN, FALSE );

    @Description( "The hostname and port to use as Prometheus endpoint" )
    public static final Setting<HostnamePort> prometheusEndpoint =
            setting( "metrics.prometheus.endpoint", HOSTNAME_PORT, "localhost:2004" );

    @Description( "Set to `true` to enable the JMX metrics endpoint" )
    public static final Setting<Boolean> jmxEnabled = setting( "metrics.jmx.enabled", BOOLEAN, TRUE );

}
