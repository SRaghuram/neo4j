/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.cluster.UniqueAddress;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.DiscoveryFirstStartupDetector;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;
import com.neo4j.causalclustering.discovery.ReplicatedRaftMapping;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreServerInfoForServerId;
import com.neo4j.causalclustering.discovery.akka.database.state.DatabaseServer;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage;
import com.neo4j.causalclustering.discovery.akka.directory.ReplicatedLeaderInfo;
import com.neo4j.causalclustering.discovery.akka.marshal.BaseAkkaSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.CoreServerInfoForServerIdSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.CoreTopologySerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.DatabaseIdWithoutNameSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.DatabaseLeaderInfoMessageSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.DatabaseServerSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.DiscoveryDatabaseStateSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.LeaderInfoSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.RaftIdSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.RaftMemberIdSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.ReadReplicaInfoSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.ReadReplicaRefreshMessageSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.ReadReplicaRemovalMessageSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.ReadReplicaTopologySerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.ReplicatedDatabaseStateSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.ReplicatedLeaderInfoSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.ReplicatedRaftMappingSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.UniqueAddressSerializer;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaRefreshMessage;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaRemovalMessage;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.configuration.CausalClusteringInternalSettings;
import com.neo4j.configuration.CausalClusteringSettings;
import com.typesafe.config.ConfigFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.database.DatabaseId;

public final class TypesafeConfigService
{
    static final String DISCOVERY_SINK_DISPATCHER = "discovery-to-neo4j-dispatcher";
    static final String DISCOVERY_DEFAULT_DISPATCHER = "default-dispatcher"; // Akka's name, do not change
    static final String LOGGING_DISPATCHER = "logging-dispatcher";

    public enum ArteryTransport
    {
        AERON( "aeron-udp" ),
        TCP( "tcp" ),
        TLS_TCP( "tls-tcp" );

        private final String configValue;

        ArteryTransport( String configValue )
        {
            this.configValue = configValue;
        }
    }

    private final DiscoveryFirstStartupDetector firstStartupDetector;
    private final Config config;
    private final ArteryTransport arteryTransport;

    public TypesafeConfigService( ArteryTransport arteryTransport, DiscoveryFirstStartupDetector firstStartupDetector, Config config )
    {
        this.firstStartupDetector = firstStartupDetector;
        this.config = config;
        this.arteryTransport = arteryTransport;
    }

    public com.typesafe.config.Config generate()
    {
        Path externalConfig = config.get( CausalClusteringInternalSettings.middleware_akka_external_config );
        final com.typesafe.config.Config base;
        if ( externalConfig == null )
        {
            base = ConfigFactory.empty();
        }
        else
        {
            base = ConfigFactory.parseFileAnySyntax( externalConfig.toFile() );
        }

        return base
                .withFallback( shutdownConfig() )
                .withFallback( transportConfig() )
                .withFallback( serializationConfig() )
                .withFallback( failureDetectorConfig() )
                .withFallback( loggingConfig() )
                .withFallback( dispatcherConfig() )
                .withFallback( clusterConfig() )
                .withFallback( ConfigFactory.defaultReference() )
                .resolve();
    }

    private com.typesafe.config.Config shutdownConfig()
    {
        HashMap<String,Object> configMap = new HashMap<>();

        configMap.put( "akka.jvm-shutdown-hooks", "off" );
        configMap.put( "akka.cluster.run-coordinated-shutdown-when-down", "off" );

        return ConfigFactory.parseMap( configMap );
    }

    private com.typesafe.config.Config transportConfig()
    {
        SocketAddress listenAddress = config.get( CausalClusteringSettings.discovery_listen_address );
        SocketAddress advertisedAddress = config.get( CausalClusteringSettings.discovery_advertised_address );
        Map<String,Object> configMap = new HashMap<>();

        configMap.put( "akka.remote.artery.enabled", true );
        configMap.put( "akka.remote.artery.transport", arteryTransport.configValue );

        configMap.put( "akka.remote.artery.canonical.hostname", hostname( advertisedAddress ) );
        configMap.put( "akka.remote.artery.canonical.port", advertisedAddress.getPort() );

        if ( config.get( CausalClusteringInternalSettings.middleware_akka_down_unreachable_on_new_joiner ) )
        {
            configMap.put( "akka.remote.artery.advanced.instruments", List.of( JoinMessageSpy.class.getCanonicalName() ) );
        }

        configMap.put( "akka.remote.artery.bind.hostname", hostname( listenAddress ) );
        configMap.put( "akka.remote.artery.bind.port", listenAddress.getPort() );

        Duration bindTimeout = config.get( CausalClusteringInternalSettings.akka_bind_timeout );
        configMap.put( "akka.remote.artery.bind.bind-timeout", bindTimeout.toMillis() + "ms" );

        // TODO: "akka.remote.artery.advanced.stop-idle-outbound-after"

        Duration connectionTimeout = config.get( CausalClusteringInternalSettings.akka_connection_timeout );
        configMap.put( "akka.remote.artery.advanced.connection-timeout", connectionTimeout.toMillis() + "ms" );

        Duration handshakeTimeout = config.get( CausalClusteringInternalSettings.akka_handshake_timeout );
        configMap.put( "akka.remote.artery.advanced.handshake-timeout", handshakeTimeout.toMillis() + "ms" );

        return ConfigFactory.parseMap( configMap );
    }

    private com.typesafe.config.Config clusterConfig()
    {
        Map<String,Object> configMap = new HashMap<>();

        int minClusterSizeAtFormation = config.get( CausalClusteringInternalSettings.middleware_akka_min_number_of_members_at_formation );
        configMap.put( "akka.cluster.min-nr-of-members", minClusterSizeAtFormation );

        Setting<Duration> seedNodeTimeoutConfiguration = firstStartupDetector.isFirstStartup() ?
                                                         CausalClusteringInternalSettings.middleware_akka_seed_node_timeout_on_first_start :
                                                         CausalClusteringInternalSettings.middleware_akka_seed_node_timeout;
        long clusterSeedTimeoutMillis = config.get( seedNodeTimeoutConfiguration ).toMillis();
        configMap.put( "akka.cluster.seed-node-timeout", clusterSeedTimeoutMillis + "ms" );

        return ConfigFactory.parseMap( configMap );
    }

    private com.typesafe.config.Config failureDetectorConfig()
    {
        Map<String,Object> configMap = new HashMap<>();

        long heartbeatIntervalMillis = config.get( CausalClusteringInternalSettings.akka_failure_detector_heartbeat_interval ).toMillis();
        configMap.put( "akka.cluster.failure-detector.heartbeat-interval", heartbeatIntervalMillis + "ms" );
        Double threshold = config.get( CausalClusteringInternalSettings.akka_failure_detector_threshold );
        configMap.put( "akka.cluster.failure-detector.threshold", threshold );
        Integer maxSampleSize = config.get( CausalClusteringInternalSettings.akka_failure_detector_max_sample_size );
        configMap.put( "akka.cluster.failure-detector.max-sample-size", maxSampleSize );
        long minStdDeviationMillis = config.get( CausalClusteringInternalSettings.akka_failure_detector_min_std_deviation ).toMillis();
        configMap.put( "akka.cluster.failure-detector.min-std-deviation", minStdDeviationMillis + "ms" );
        long acceptableHeartbeatPauseMillis = config.get( CausalClusteringInternalSettings.akka_failure_detector_acceptable_heartbeat_pause ).toMillis();
        configMap.put( "akka.cluster.failure-detector.acceptable-heartbeat-pause", acceptableHeartbeatPauseMillis + "ms" );
        Integer monitoredByNrOfMembers = config.get( CausalClusteringInternalSettings.akka_failure_detector_monitored_by_nr_of_members );
        configMap.put( "akka.cluster.failure-detector.monitored-by-nr-of-members", monitoredByNrOfMembers );
        long expectedResponseAfterMillis = config.get( CausalClusteringInternalSettings.akka_failure_detector_expected_response_after ).toMillis();
        configMap.put( "akka.cluster.failure-detector.expected-response-after", expectedResponseAfterMillis + "ms" );

        return ConfigFactory.parseMap( configMap );
    }

    private com.typesafe.config.Config dispatcherConfig()
    {
        Map<String,Object> configMap = new HashMap<>();

        Integer sinkParallelism = config.get( CausalClusteringInternalSettings.middleware_akka_sink_parallelism_level );
        forkJoinDispatcher( configMap, sinkParallelism, DISCOVERY_SINK_DISPATCHER );

        Integer defaultParallelism = config.get( CausalClusteringInternalSettings.middleware_akka_default_parallelism_level );
        forkJoinDispatcher( configMap, defaultParallelism, DISCOVERY_DEFAULT_DISPATCHER );

        configMap.put( LOGGING_DISPATCHER + ".type", "Dispatcher" );
        configMap.put( LOGGING_DISPATCHER + ".executor", "thread-pool-executor" );
        configMap.put( LOGGING_DISPATCHER + ".thread-pool-executor.fixed-pool-size", 1 );
        configMap.put( LOGGING_DISPATCHER + ".throughput", 1 );

        return ConfigFactory.parseMap( configMap );
    }

    private void forkJoinDispatcher( Map<String,Object> configMap, Integer parallelism, String dispatcherName )
    {
        // parallelism is processors * parallelism-factor, bounded between parallelism-min and parallelism-max
        configMap.put( dispatcherName + ".type", "Dispatcher" );
        configMap.put( dispatcherName + ".executor", "fork-join-executor" );
        configMap.put( dispatcherName + ".fork-join-executor.parallelism-min", parallelism );
        configMap.put( dispatcherName + ".fork-join-executor.parallelism-factor", 1.0 );
        configMap.put( dispatcherName + ".fork-join-executor.parallelism-max", parallelism );
        configMap.put( dispatcherName + ".throughput", 10 );
    }

    static String hostname( SocketAddress socketAddress )
    {
        if ( socketAddress.isIPv6() )
        {
            return "[" + socketAddress.getHostname() + "]";
        }
        else
        {
            return socketAddress.getHostname().toLowerCase();
        }
    }

    private com.typesafe.config.Config serializationConfig()
    {
        HashMap<String,Object> configMap = new HashMap<>();

        configMap.put( "akka.actor.allow-java-serialization", "off" );

        addSerializer( LeaderInfo.class, LeaderInfoSerializer.class, configMap );
        addSerializer( RaftGroupId.class, RaftIdSerializer.class, configMap );
        addSerializer( UniqueAddress.class, UniqueAddressSerializer.class, configMap );
        addSerializer( CoreServerInfoForServerId.class, CoreServerInfoForServerIdSerializer.class, configMap );
        addSerializer( ReadReplicaRefreshMessage.class, ReadReplicaRefreshMessageSerializer.class, configMap );
        addSerializer( RaftMemberId.class, RaftMemberIdSerializer.class, configMap );
        addSerializer( ReadReplicaInfo.class, ReadReplicaInfoSerializer.class, configMap );
        addSerializer( DatabaseCoreTopology.class, CoreTopologySerializer.class, configMap );
        addSerializer( ReadReplicaRemovalMessage.class, ReadReplicaRemovalMessageSerializer.class, configMap );
        addSerializer( DatabaseReadReplicaTopology.class, ReadReplicaTopologySerializer.class, configMap );
        addSerializer( LeaderInfoDirectoryMessage.class, DatabaseLeaderInfoMessageSerializer.class, configMap );
        addSerializer( ReplicatedLeaderInfo.class, ReplicatedLeaderInfoSerializer.class, configMap );
        addSerializer( DatabaseId.class, DatabaseIdWithoutNameSerializer.class, configMap );
        addSerializer( ReplicatedDatabaseState.class, ReplicatedDatabaseStateSerializer.class, configMap );
        addSerializer( DatabaseServer.class, DatabaseServerSerializer.class, configMap );
        addSerializer( DiscoveryDatabaseState.class, DiscoveryDatabaseStateSerializer.class, configMap );
        addSerializer( ReplicatedRaftMapping.class, ReplicatedRaftMappingSerializer.class, configMap );

        return ConfigFactory.parseMap( configMap );
    }

    private <T, M extends BaseAkkaSerializer<T>> void addSerializer( Class<T> message, Class<M> serializer, Map<String,Object> configMap )
    {
        String customSerializer = message.getSimpleName() + "-serializer";
        configMap.put( "akka.actor.serializers." + customSerializer, serializer.getCanonicalName() );
        configMap.put( "akka.actor.serialization-bindings.\"" + message.getCanonicalName() + "\"", customSerializer );
    }

    private com.typesafe.config.Config loggingConfig()
    {
        HashMap<String,Object> configMap = new HashMap<>();
        configMap.put( "akka.loggers", Collections.singletonList( LoggingActor.class.getCanonicalName() ) );
        configMap.put( "akka.loglevel", logLevel( config ).toString() );
        configMap.put( "akka.logging-filter", LoggingFilter.class.getCanonicalName() );
        configMap.put( "akka.loggers-dispatcher", LOGGING_DISPATCHER );
        configMap.put( "akka.logger-startup-timeout", "15s" );

        return ConfigFactory.parseMap( configMap );
    }

    private static AkkaLoggingLevel logLevel( Config config )
    {
        var configuredLevel = config.get( CausalClusteringSettings.middleware_logging_level );
        return AkkaLoggingLevel.fromNeo4jLevel( configuredLevel );
    }

}
