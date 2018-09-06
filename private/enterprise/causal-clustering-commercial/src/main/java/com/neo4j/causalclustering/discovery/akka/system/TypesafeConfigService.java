/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.cluster.UniqueAddress;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage;
import com.typesafe.config.ConfigFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.HostnameResolver;
import org.neo4j.causalclustering.discovery.ReadReplicaInfo;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreServerInfoForMemberId;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaInfoMessage;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaRemovalMessage;
import com.neo4j.causalclustering.discovery.akka.marshal.BaseAkkaSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.ClusterIdSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.CoreServerInfoForMemberIdSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.CoreTopologySerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.DatabaseLeaderInfoMessageSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.LeaderInfoSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.MemberIdSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.ReadReplicaInfoMessageSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.ReadReplicaInfoSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.ReadReplicaRemovalMessageSerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.ReadReplicaTopologySerializer;
import com.neo4j.causalclustering.discovery.akka.marshal.UniqueAddressSerializer;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;

public final class TypesafeConfigService
{
    public static final String AKKA_SCHEME = "akka://";

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

    private final Config config;
    private final ArteryTransport arteryTransport;
    private final HostnameResolver hostnameResolver;

    public TypesafeConfigService( HostnameResolver hostnameResolver, ArteryTransport arteryTransport, Config config )
    {
        this.config = config;
        this.arteryTransport = arteryTransport;
        this.hostnameResolver = hostnameResolver;
    }

    public com.typesafe.config.Config generate()
    {
        return ConfigFactory.empty()
                .withFallback( clusterConfig() )
                .withFallback( transportConfig() )
                .withFallback( serializationConfig() )
                .withFallback( failureDetectorConfig() )
                .withFallback( loggingConfig() )
                .withFallback( ConfigFactory.defaultReference() )
                .resolve();
    }

    private com.typesafe.config.Config clusterConfig()
    {
        List<String> seedAkkaClusterNodes = initialActorSystemPaths();

        EnterpriseEditionSettings.Mode mode = config.get( EnterpriseEditionSettings.mode );

        Map<String,Object> configMap = new HashMap<>();
        configMap.put( "akka.cluster.seed-nodes", seedAkkaClusterNodes );
        configMap.put( "akka.cluster.roles", Collections.singletonList( mode.name() ) );
        return ConfigFactory.parseMap( configMap );
    }

    List<String> initialActorSystemPaths()
    {
        List<AdvertisedSocketAddress> initMembers = config.get( CausalClusteringSettings.initial_discovery_members );
        return initMembers.stream()
                .flatMap( rawAddress -> hostnameResolver.resolve( rawAddress ).stream() )
                .map( resolvedAddress -> AKKA_SCHEME + ActorSystemFactory.ACTOR_SYSTEM_NAME + "@" + resolvedAddress )
                .collect( Collectors.toList() );
    }

    private com.typesafe.config.Config transportConfig()
    {
        ListenSocketAddress listenAddress = config.get( CausalClusteringSettings.discovery_listen_address );
        AdvertisedSocketAddress advertisedAddress = config.get( CausalClusteringSettings.discovery_advertised_address );
        Map<String,Object> configMap = new HashMap<>();

        configMap.put( "akka.remote.artery.enabled", true );
        configMap.put( "akka.remote.artery.transport", arteryTransport.configValue );

        configMap.put( "akka.remote.artery.canonical.hostname", hostname( advertisedAddress ) );
        configMap.put( "akka.remote.artery.canonical.port", advertisedAddress.getPort() );

        configMap.put( "akka.remote.artery.bind.hostname", hostname( listenAddress ) );
        configMap.put( "akka.remote.artery.bind.port", listenAddress.getPort() );

        return ConfigFactory.parseMap( configMap );
    }

    private com.typesafe.config.Config failureDetectorConfig()
    {
        Map<String,Object> configMap = new HashMap<>();

        long heartbeatIntervalMillis = config.get( CausalClusteringSettings.akka_failure_detector_heartbeat_interval ).toMillis();
        configMap.put( "akka.cluster.failure-detector.heartbeat-interval", heartbeatIntervalMillis + "ms" );
        Double threshold = config.get( CausalClusteringSettings.akka_failure_detector_threshold );
        configMap.put( "akka.cluster.failure-detector.threshold", threshold );
        Integer maxSampleSize = config.get( CausalClusteringSettings.akka_failure_detector_max_sample_size );
        configMap.put( "akka.cluster.failure-detector.max-sample-size", maxSampleSize );
        long minStdDeviationMillis = config.get( CausalClusteringSettings.akka_failure_detector_min_std_deviation ).toMillis();
        configMap.put( "akka.cluster.failure-detector.min-std-deviation", minStdDeviationMillis + "ms" );
        long acceptableHeartbeatPauseMillis = config.get( CausalClusteringSettings.akka_failure_detector_acceptable_heartbeat_pause ).toMillis();
        configMap.put( "akka.cluster.failure-detector.acceptable-heartbeat-pause", acceptableHeartbeatPauseMillis + "ms" );
        Integer monitoredByNrOfMembers = config.get( CausalClusteringSettings.akka_failure_detector_monitored_by_nr_of_members );
        configMap.put( "akka.cluster.failure-detector.monitored-by-nr-of-members", monitoredByNrOfMembers );
        long expectedResponseAfterMillis = config.get( CausalClusteringSettings.akka_failure_detector_expected_response_after ).toMillis();
        configMap.put( "akka.cluster.failure-detector.expected-response-after", expectedResponseAfterMillis + "ms" );

        return ConfigFactory.parseMap( configMap );
    }

    private String hostname( SocketAddress socketAddress )
    {
        if ( socketAddress.isIPv6() )
        {
            return "[" + socketAddress.getHostname() + "]";
        }
        else
        {
            return socketAddress.getHostname();
        }
    }

    private com.typesafe.config.Config serializationConfig()
    {
        HashMap<String,Object> configMap = new HashMap<>();

        configMap.put( "akka.actor.allow-java-serialization", "off" );

        addSerializer( LeaderInfo.class, LeaderInfoSerializer.class, configMap );
        addSerializer( ClusterId.class, ClusterIdSerializer.class, configMap );
        addSerializer( UniqueAddress.class, UniqueAddressSerializer.class, configMap );
        addSerializer( CoreServerInfoForMemberId.class, CoreServerInfoForMemberIdSerializer.class, configMap );
        addSerializer( ReadReplicaInfoMessage.class, ReadReplicaInfoMessageSerializer.class, configMap );
        addSerializer( MemberId.class, MemberIdSerializer.class, configMap );
        addSerializer( ReadReplicaInfo.class, ReadReplicaInfoSerializer.class, configMap );
        addSerializer( CoreTopology.class, CoreTopologySerializer.class, configMap );
        addSerializer( ReadReplicaRemovalMessage.class, ReadReplicaRemovalMessageSerializer.class, configMap );
        addSerializer( ReadReplicaTopology.class, ReadReplicaTopologySerializer.class, configMap );
        addSerializer( LeaderInfoDirectoryMessage.class, DatabaseLeaderInfoMessageSerializer.class, configMap );

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
        configMap.put( "akka.loglevel", logLevel( config ) );
        configMap.put( "akka.logging-filter", LoggingFilter.class.getCanonicalName() );

        return ConfigFactory.parseMap( configMap );
    }

    private String logLevel( Config config )
    {
        Boolean disableLogging = config.get( CausalClusteringSettings.disable_middleware_logging );
        Integer level = config.get( CausalClusteringSettings.middleware_logging_level );
        if ( disableLogging )
        {
            return "OFF";
        }
        else if ( level <= Level.FINE.intValue() )
        {
            return "DEBUG";
        }
        else if ( level <= Level.INFO.intValue() )
        {
            return "INFO";
        }
        else if ( level <= Level.WARNING.intValue() )
        {
            return "WARN";
        }
        else
        {
            return "ERROR";
        }
    }

}
