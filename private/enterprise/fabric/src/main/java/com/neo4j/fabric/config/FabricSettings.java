/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.config;

import java.net.URI;
import java.time.Duration;
import java.util.List;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.GroupSetting;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.logging.Level;

import static java.time.Duration.ofMinutes;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.LONG;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;

@ServiceProvider
public class FabricSettings implements SettingsDeclaration
{
    private static final String DRIVER_LOGGING_LEVEL = "driver.logging.level";
    private static final String DRIVER_LOG_LEAKED_SESSIONS = "driver.logging.leaked_sessions";
    private static final String DRIVER_MAX_CONNECTION_POOL_SIZE = "driver.connection.pool.max_size";
    private static final String DRIVER_IDLE_TIME_BEFORE_CONNECTION_TEST = "driver.connection.pool.idle_test";
    private static final String DRIVER_CONNECTION_ACQUISITION_TIMEOUT = "driver.connection.pool.acquisition_timeout";
    private static final String DRIVER_MAX_CONNECTION_LIFETIME = "driver.connection.max_lifetime";
    private static final String DRIVER_ENCRYPTED = "driver.connection.encrypted";
    private static final String DRIVER_CONNECT_TIMEOUT = "driver.connection.connect_timeout";
    private static final String DRIVER_TRUST_STRATEGY = "driver.trust_strategy";
    private static final String DRIVER_LOAD_BALANCING_STRATEGY = "driver.load_balancing_strategy";
    private static final String DRIVER_RETRY_MAX_TIME = "driver.retry_timeout";
    private static final String DRIVER_METRICS_ENABLED = "driver.metrics.enabled";
    private static final String DRIVER_API = "driver.api";

    static Setting<List<SocketAddress>> fabricServersSetting = newBuilder( "fabric.routing.servers",
            SettingValueParsers.listOf( SettingValueParsers.SOCKET_ADDRESS ),
            List.of(new SocketAddress( "localhost", 7687 )))
            .build();
    static Setting<String> databaseName = newBuilder( "fabric.database.name", STRING, null ).build();
    static Setting<Long> routingTtlSetting = newBuilder( "fabric.routing.ttl", LONG, 1000L ).build();

    static Setting<Duration> driverIdleTimeout = newBuilder( "fabric.driver.timeout", DURATION, ofMinutes( 1 ) ).build();
    static Setting<Duration> driverIdleCheckInterval = newBuilder( "fabric.driver.idle.check.interval", DURATION, ofMinutes( 1 ) ).build();
    static Setting<Integer> driverEventLoopCount = newBuilder( "fabric.driver.event.loop.count", INT, Runtime.getRuntime().availableProcessors() ).build();

    static Setting<Level> driverLoggingLevel = newBuilder( "fabric." + DRIVER_LOGGING_LEVEL, ofEnum(Level.class), null ).build();
    static Setting<Boolean> driverLogLeakedSessions = newBuilder( "fabric." + DRIVER_LOG_LEAKED_SESSIONS, BOOL, null ).build();
    static Setting<Integer> driverMaxConnectionPoolSize = newBuilder( "fabric." + DRIVER_MAX_CONNECTION_POOL_SIZE, INT, null ).build();
    static Setting<Duration> driverIdleTimeBeforeConnectionTest = newBuilder( "fabric." + DRIVER_IDLE_TIME_BEFORE_CONNECTION_TEST, DURATION, null ).build();
    static Setting<Duration> driverMaxConnectionLifetime = newBuilder( "fabric." + DRIVER_MAX_CONNECTION_LIFETIME, DURATION, null ).build();
    static Setting<Duration> driverConnectionAcquisitionTimeout = newBuilder( "fabric." + DRIVER_CONNECTION_ACQUISITION_TIMEOUT, DURATION, null ).build();
    static Setting<Boolean> driverEncrypted = newBuilder( "fabric." + DRIVER_ENCRYPTED, BOOL, null ).build();
    static Setting<DriverTrustStrategy> driverTrustStrategy =
            newBuilder( "fabric." + DRIVER_TRUST_STRATEGY, ofEnum( DriverTrustStrategy.class ), null ).build();
    static Setting<DriverLoadBalancingStrategy> driverLoadBalancingStrategy =
            newBuilder( "fabric." + DRIVER_LOAD_BALANCING_STRATEGY, ofEnum( DriverLoadBalancingStrategy.class ), null ).build();
    static Setting<Duration> driverConnectTimeout = newBuilder( "fabric." + DRIVER_CONNECT_TIMEOUT, DURATION, null ).build();
    static Setting<Duration> driverRetryMaxTime = newBuilder( "fabric." + DRIVER_RETRY_MAX_TIME, DURATION, null ).build();
    static Setting<Boolean> driverMetricsEnabled = newBuilder( "fabric." + DRIVER_METRICS_ENABLED, BOOL, null ).build();

    static Setting<Integer> bufferLowWatermarkSetting = newBuilder( "fabric.stream.buffer.low.watermark",
            INT,
            300 )
            .build();
    static Setting<Integer> bufferSizeSetting = newBuilder( "fabric.stream.buffer.size", INT, 1000 ).build();
    static Setting<Integer> syncBatchSizeSetting = newBuilder( "fabric.stream.sync.batch.size", INT, 50 ).build();
    static Setting<DriverApi> driverApi = newBuilder( "fabric." + DRIVER_API, ofEnum(DriverApi.class), DriverApi.RX ).build();

    @ServiceProvider
    public static class GraphSetting extends GroupSetting
    {

        public final Setting<URI> uri = getBuilder( "uri", SettingValueParsers.URI, null ).build();
        public final Setting<String> database = getBuilder( "database", SettingValueParsers.STRING, null ).build();
        public final Setting<String> name = getBuilder( "name", SettingValueParsers.STRING, null ).build();

        public final Setting<Level> driverLoggingLevel = getBuilder( DRIVER_LOGGING_LEVEL, ofEnum(Level.class), null ).build();
        public final Setting<Boolean> driverLogLeakedSessions = getBuilder( DRIVER_LOG_LEAKED_SESSIONS, BOOL, null ).build();
        public final Setting<Integer> driverMaxConnectionPoolSize = getBuilder( DRIVER_MAX_CONNECTION_POOL_SIZE, INT, null ).build();
        public final Setting<Duration> driverIdleTimeBeforeConnectionTest = getBuilder( DRIVER_IDLE_TIME_BEFORE_CONNECTION_TEST, DURATION, null ).build();
        public final Setting<Duration> driverMaxConnectionLifetime = getBuilder( DRIVER_MAX_CONNECTION_LIFETIME, DURATION, null ).build();
        public final Setting<Duration> driverConnectionAcquisitionTimeout = getBuilder( DRIVER_CONNECTION_ACQUISITION_TIMEOUT, DURATION, null ).build();
        public final Setting<Boolean> driverEncrypted = getBuilder( DRIVER_ENCRYPTED, BOOL, null ).build();
        public final Setting<DriverTrustStrategy> driverTrustStrategy = getBuilder( DRIVER_TRUST_STRATEGY, ofEnum( DriverTrustStrategy.class ), null ).build();
        public final Setting<DriverLoadBalancingStrategy> driverLoadBalancingStrategy =
                getBuilder( DRIVER_LOAD_BALANCING_STRATEGY, ofEnum( DriverLoadBalancingStrategy.class ), null ).build();
        public final Setting<Duration> driverConnectTimeout = getBuilder( DRIVER_CONNECT_TIMEOUT, DURATION, null ).build();
        public final Setting<Duration> driverRetryMaxTime = getBuilder( DRIVER_RETRY_MAX_TIME, DURATION, null ).build();
        public final Setting<Boolean> driverMetricsEnabled = getBuilder( DRIVER_METRICS_ENABLED, BOOL, null ).build();
        public final Setting<DriverApi> driverApi = getBuilder( DRIVER_API, ofEnum(DriverApi.class), null ).build();

        protected GraphSetting( String name )
        {
            super( name );
        }

        public GraphSetting()
        {
            super( null ); // For ServiceLoader
        }

        @Override
        public String getPrefix()
        {
            return "fabric.graph";
        }
    }

    public enum DriverTrustStrategy
    {
        TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
        TRUST_ALL_CERTIFICATES
    }

    public enum DriverLoadBalancingStrategy
    {
        ROUND_ROBIN,
        LEAST_CONNECTED
    }

    public enum DriverApi
    {
        RX,
        ASYNC
    }
}
