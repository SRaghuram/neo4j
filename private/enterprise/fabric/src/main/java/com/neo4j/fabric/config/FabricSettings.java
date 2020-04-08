/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.config;

import com.neo4j.fabric.driver.DriverConfigFactory;

import java.net.URI;
import java.time.Duration;
import java.util.List;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.DocumentedDefaultValue;
import org.neo4j.configuration.GroupSetting;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.configuration.helpers.NormalizedGraphName;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.logging.Level;

import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.DATABASENAME;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;

@SuppressWarnings( "WeakerAccess" )
@ServiceProvider
public class FabricSettings implements SettingsDeclaration
{
    private static final String DRIVER_LOGGING_LEVEL = "driver.logging.level";
    private static final String DRIVER_LOG_LEAKED_SESSIONS = "driver.logging.leaked_sessions";
    private static final String DRIVER_MAX_CONNECTION_POOL_SIZE = "driver.connection.pool.max_size";
    private static final String DRIVER_IDLE_TIME_BEFORE_CONNECTION_TEST = "driver.connection.pool.idle_test";
    private static final String DRIVER_CONNECTION_ACQUISITION_TIMEOUT = "driver.connection.pool.acquisition_timeout";
    private static final String DRIVER_MAX_CONNECTION_LIFETIME = "driver.connection.max_lifetime";
    private static final String DRIVER_CONNECT_TIMEOUT = "driver.connection.connect_timeout";
    private static final String DRIVER_API = "driver.api";

    @Description( "A comma-separated list of Fabric instances that form a routing group. " +
            "A driver will route transactions to available routing group members.\n" +
            "A Fabric instance is represented by its Bolt connector address." )
    public static final Setting<List<SocketAddress>> fabricServersSetting = newBuilder( "fabric.routing.servers",
            SettingValueParsers.listOf( SettingValueParsers.SOCKET_ADDRESS ),
            List.of(new SocketAddress( "localhost", 7687 )))
            .dynamic()
            .build();

    @Description( "Name of the Fabric database. Only one Fabric database is currently supported per Neo4j instance." )
    public static final Setting<String> databaseName = newBuilder( "fabric.database.name", DATABASENAME, null ).build();

    @Description( "The time to live (TTL) of a routing table for fabric routing group." )
    public static final Setting<Duration> routingTtlSetting = newBuilder( "fabric.routing.ttl", DURATION, ofMinutes( 1 )  ).build();

    @Description( "Time interval of inactivity after which a driver will be closed." )
    @Internal
    public static final Setting<Duration> driverIdleTimeout = newBuilder( "fabric.driver.timeout", DURATION, ofMinutes( 1 ) ).build();

    @Description( "Time interval between driver idleness check." )
    @Internal
    public static final Setting<Duration> driverIdleCheckInterval = newBuilder( "fabric.driver.idle_check_interval", DURATION, ofMinutes( 1 ) ).build();

    @Description( " Number of event loops used by drivers. Event loops are shard between drivers, so this is the total number of event loops created." )
    @DocumentedDefaultValue( "Number of available processors" )
    @Internal
    public static final Setting<Integer> driverEventLoopCount =
            newBuilder( "fabric.driver.event_loop_count", INT, Runtime.getRuntime().availableProcessors() ).build();

    @Description( "Sets level for driver internal logging." )
    @DocumentedDefaultValue( "Value of dbms.logs.debug.level" )
    public static final Setting<Level> driverLoggingLevel = newBuilder( "fabric." + DRIVER_LOGGING_LEVEL, ofEnum(Level.class), null ).build();

    @Description( "Enables logging of leaked driver session" )
    @Internal
    public static final Setting<Boolean> driverLogLeakedSessions = newBuilder( "fabric." + DRIVER_LOG_LEAKED_SESSIONS, BOOL, false ).build();

    @Description( "Maximum total number of connections to be managed by a connection pool.\n" +
            "The limit is enforced for a combination of a host and user. Negative values are allowed and result in unlimited pool. Value of 0" +
            "is not allowed." )
    @DocumentedDefaultValue( "Unlimited" )
    public static final Setting<Integer> driverMaxConnectionPoolSize = newBuilder( "fabric." + DRIVER_MAX_CONNECTION_POOL_SIZE, INT, -1 ).build();

    @Description( "Pooled connections that have been idle in the pool for longer than this timeout " +
            "will be tested before they are used again, to ensure they are still alive.\n" +
            "If this option is set too low, an additional network call will be incurred when acquiring a connection, which causes a performance hit.\n" +
            "If this is set high, no longer live connections might be used which might lead to errors.\n" +
            "Hence, this parameter tunes a balance between the likelihood of experiencing connection problems and performance\n" +
            "Normally, this parameter should not need tuning.\n" +
            "Value 0 means connections will always be tested for validity" )
    @DocumentedDefaultValue(  "No connection liveliness check is done by default." )
    public static final Setting<Duration> driverIdleTimeBeforeConnectionTest = newBuilder( "fabric." + DRIVER_IDLE_TIME_BEFORE_CONNECTION_TEST, DURATION, null )
            .build();

    @Description( "Pooled connections older than this threshold will be closed and removed from the pool.\n" +
            "Setting this option to a low value will cause a high connection churn and might result in a performance hit.\n" +
            "It is recommended to set maximum lifetime to a slightly smaller value than the one configured in network\n" +
            "equipment (load balancer, proxy, firewall, etc. can also limit maximum connection lifetime).\n" +
            "Zero and negative values result in lifetime not being checked." )
    public static final Setting<Duration> driverMaxConnectionLifetime =
            newBuilder( "fabric." + DRIVER_MAX_CONNECTION_LIFETIME, DURATION, Duration.ofHours( 1 ) ).build();

    @Description( "Maximum amount of time spent attempting to acquire a connection from the connection pool.\n" +
            "This timeout only kicks in when all existing connections are being used and no new " +
            "connections can be created because maximum connection pool size has been reached.\n" +
            "Error is raised when connection can't be acquired within configured time.\n" +
            "Negative values are allowed and result in unlimited acquisition timeout. Value of 0 is allowed " +
            "and results in no timeout and immediate failure when connection is unavailable" )
    public static final Setting<Duration> driverConnectionAcquisitionTimeout =
            newBuilder( "fabric." + DRIVER_CONNECTION_ACQUISITION_TIMEOUT, DURATION, ofSeconds( 60 ) ).build();

    @Description( "Socket connection timeout.\n" +
            "A timeout of zero is treated as an infinite timeout and will be bound by the timeout configured on the\n" +
            "operating system level." )
    public static final Setting<Duration> driverConnectTimeout = newBuilder( "fabric." + DRIVER_CONNECT_TIMEOUT, DURATION, ofSeconds( 5 ) ).build();

    @Description( "Maximal size of a buffer used for pre-fetching result records of remote queries.\n" +
            "To compensate for latency to remote databases, the Fabric execution engine pre-fetches records needed for local executions.\n" +
            "This limit is enforced per fabric query. If a fabric query uses multiple remote stream at the same time, " +
            "this setting represents the maximal number of pre-fetched records counted together for all such remote streams" )
    public static final Setting<Integer> bufferSizeSetting = newBuilder( "fabric.stream.buffer.size", INT, 1000 )
            .addConstraint( min(1) )
            .build();

    @Description( "Number of records in prefetching buffer that will trigger prefetching again. This is strongly related to fabric.stream.buffer.size" )
    public static final Setting<Integer> bufferLowWatermarkSetting = newBuilder( "fabric.stream.buffer.low_watermark",
            INT,
            300 )
            .addConstraint( min(0) )
            .build();

    @Description( "Batch size used when requesting records from local Cypher engine." )
    @Internal
    public static final Setting<Integer> batchSizeSetting = newBuilder( "fabric.stream.batch_size", INT, 50 )
            .addConstraint( min(1) )
            .build();

    @Description( "Maximal concurrency within Fabric queries.\n" +
            "Limits the number of iterations of each subquery that are executed concurrently. " +
            "Higher concurrency may consume more memory and network resources simultaneously, " +
            "while lower concurrency may force sequential execution, requiring more time." )
    @DocumentedDefaultValue( "The number of remote graphs" )
    public static final Setting<Integer> concurrency = newBuilder( "fabric.stream.concurrency", INT, null )
            .addConstraint( min(1) )
            .build();

    @Description( "Determines which driver API will be used. ASYNC must be used when the remote instance is 3.5" )
    public static final Setting<DriverConfigFactory.DriverApi> driverApi = newBuilder( "fabric." + DRIVER_API,
            ofEnum(DriverConfigFactory.DriverApi.class),
            DriverConfigFactory.DriverApi.RX )
            .build();

    @ServiceProvider
    public static class GraphSetting extends GroupSetting
    {
        @Description( "URI of the Neo4j DBMS hosting the database associated to the Fabric graph. Example: neo4j://somewhere:7687 \n" +
                "A comma separated list of URIs is acceptable. This is useful when the Fabric graph is hosted on a cluster" +
                "and more that one bootstrap address needs to be provided in order to avoid a single point of failure." +
                "The provided addresses will be considered as an initial source of a routing table." +
                "Example: neo4j://core-1:1111,neo4j://core-2:2222" )
        public final Setting<List<URI>> uris = getBuilder( "uri", SettingValueParsers.listOf( SettingValueParsers.URI ), null ).build();

        @Description( "Name of the database associated to the Fabric graph." )
        @DocumentedDefaultValue( "The default database on the target DBMS. Typically 'Neo4j'" )
        public final Setting<String> database = getBuilder( "database", SettingValueParsers.STRING, null ).build();

        @Description( "Name assigned to the Fabric graph. The name can be used in Fabric queries." )
        public final Setting<NormalizedGraphName> name = getBuilder( "name", SettingValueParsers.GRAPHNAME, null ).build();

        public final Setting<Level> driverLoggingLevel = getBuilder( DRIVER_LOGGING_LEVEL, ofEnum(Level.class), null ).build();
        public final Setting<Boolean> driverLogLeakedSessions = getBuilder( DRIVER_LOG_LEAKED_SESSIONS, BOOL, null ).build();
        public final Setting<Integer> driverMaxConnectionPoolSize = getBuilder( DRIVER_MAX_CONNECTION_POOL_SIZE, INT, null ).build();
        public final Setting<Duration> driverIdleTimeBeforeConnectionTest = getBuilder( DRIVER_IDLE_TIME_BEFORE_CONNECTION_TEST, DURATION, null ).build();
        public final Setting<Duration> driverMaxConnectionLifetime = getBuilder( DRIVER_MAX_CONNECTION_LIFETIME, DURATION, null ).build();
        public final Setting<Duration> driverConnectionAcquisitionTimeout = getBuilder( DRIVER_CONNECTION_ACQUISITION_TIMEOUT, DURATION, null ).build();
        public final Setting<Duration> driverConnectTimeout = getBuilder( DRIVER_CONNECT_TIMEOUT, DURATION, null ).build();
        public final Setting<DriverConfigFactory.DriverApi> driverApi = getBuilder( DRIVER_API,
                ofEnum( DriverConfigFactory.DriverApi.class ),
                null )
                .build();

        @Description( "SSL for Fabric drivers is configured using 'fabric' SSL policy." +
                "This setting can be used to instruct the driver not to use SSL even though 'fabric' SSL policy is configured." +
                "The driver will use SSL if 'fabric' SSL policy is configured and this setting is set to 'true'" )
        public final Setting<Boolean> sslEnabled = getBuilder( "driver.ssl_enabled", BOOL, true ).build();

        public static GraphSetting of( String name )
        {
            return new GraphSetting( name );
        }

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
}
