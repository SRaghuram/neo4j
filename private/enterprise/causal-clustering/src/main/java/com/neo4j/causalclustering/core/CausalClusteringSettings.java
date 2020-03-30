/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCacheFactory;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocolVersion;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocols;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingValueParser;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.configuration.helpers.DurationRange;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.logging.Level;

import static com.neo4j.causalclustering.core.ServerGroupName.SERVER_GROUP_NAME;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static java.util.Collections.emptyList;
import static org.neo4j.configuration.GraphDatabaseSettings.default_advertised_address;
import static org.neo4j.configuration.GraphDatabaseSettings.default_listen_address;
import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingConstraints.range;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.configuration.SettingValueParsers.DOUBLE;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.DURATION_RANGE;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import static org.neo4j.configuration.SettingValueParsers.listOf;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;

@Description( "Settings for Causal Clustering" )
@ServiceProvider
public class CausalClusteringSettings implements SettingsDeclaration
{
    public static final String TEMP_STORE_COPY_DIRECTORY_NAME = "temp-copy";
    public static final String TEMP_BOOTSTRAP_DIRECTORY_NAME = "temp-bootstrap";
    public static final String TEMP_SAVE_DIRECTORY_NAME = "temp-save";

    @Description( "Time out for a new member to catch up" )
    public static final Setting<Duration> join_catch_up_timeout =
            newBuilder( "causal_clustering.join_catch_up_timeout", DURATION, ofMinutes( 10 ) ).build();

    @Deprecated
    @Description( "This setting is moved and enhanced into causal_clustering.failure_detection_window and causal_clustering.failure_resolution_window." )
    public static final Setting<Duration> leader_election_timeout =
            newBuilder( "causal_clustering.leader_election_timeout", DURATION, ofSeconds( 7 ) ).build();

    @Description( "The time window within which the loss of the leader is detected and the first re-election attempt is held." )
    public static final Setting<DurationRange> failure_detection_window =
            newBuilder( "causal_clustering.failure_detection_window", DURATION_RANGE, DurationRange.fromSeconds( 7, 10 ) ).build();

    @Description( "The rate at which leader elections happen. Note that due to election conflicts it might take several attempts to find a leader. " +
            "The window should be significantly larger than typical communication delays to make conflicts unlikely." )
    public static final Setting<DurationRange> failure_resolution_window =
            newBuilder( "causal_clustering.failure_resolution_window", DURATION_RANGE, DurationRange.fromSeconds( 3, 6 ) ).build();

    @Internal
    @Description( "The time limit within which a leadership transfer request should be completed, otherwise the leader will resume accepting writes." )
    public static final Setting<Duration> leader_transfer_timeout =
            newBuilder( "causal_clustering.leader_transfer_timeout", DURATION, ofSeconds( 3 ) ).build();

    @Internal
    @Description( "The frequency with which a leader will try and transfer leadership to another member" )
    public static final Setting<Duration> leader_transfer_interval =
            newBuilder( "causal_clustering.leader_transfer_interval", DURATION, ofSeconds( 15 ) ).build();

    @Internal
    @Description( "The amount of time we should wait before transferring the leadership of a given database to a member after that member rejects a " +
                  "previous transfer." )
    public static final Setting<Duration> leader_transfer_member_backoff =
            newBuilder( "causal_clustering.leader_transfer_member_backoff", DURATION, ofSeconds( 30 ) ).build();

    @Internal
    @Description( "Configures the time after which we give up trying to bind to a cluster formed of the other initial discovery members." )
    public static final Setting<Duration> cluster_binding_timeout =
            newBuilder( "causal_clustering.cluster_binding_timeout", DURATION, ofMinutes( 5 ) ).build();

    @Internal
    @Description( "Configures the time after which we retry binding to a cluster. Only applies to Akka discovery. " +
            "A discovery type of DNS/SRV/K8S will be queried again on retry." )
    public static final Setting<Duration> cluster_binding_retry_timeout =
            newBuilder( "causal_clustering.cluster_binding_retry_timeout", DURATION, ofMinutes( 1 ) ).build();

    @Description( "Prevents the current instance from volunteering to become Raft leader. Defaults to false, and " +
            "should only be used in exceptional circumstances by expert users. Using this can result in reduced " +
            "availability for the cluster." )
    public static final Setting<Boolean> refuse_to_be_leader =
            newBuilder( "causal_clustering.refuse_to_be_leader", BOOL, false ).build();

    @Description( "Enable pre-voting extension to the Raft protocol (this is breaking and must match between the core cluster members)" )
    public static final Setting<Boolean> enable_pre_voting =
            newBuilder( "causal_clustering.enable_pre_voting", BOOL, true ).build();

    @Description( "The maximum batch size when catching up (in unit of entries)" )
    public static final Setting<Integer> catchup_batch_size =
            newBuilder( "causal_clustering.catchup_batch_size", INT, 64 ).build();

    @Description( "The maximum lag allowed before log shipping pauses (in unit of entries)" )
    public static final Setting<Integer> log_shipping_max_lag =
            newBuilder( "causal_clustering.log_shipping_max_lag", INT, 256 ).build();

    @Internal
    @Description( "Maximum number of entries in the RAFT in-queue" )
    public static final Setting<Integer> raft_in_queue_size =
            newBuilder( "causal_clustering.raft_in_queue_size", INT, 1024 ).build();

    @Description( "Maximum number of bytes in the RAFT in-queue" )
    public static final Setting<Long> raft_in_queue_max_bytes =
            newBuilder( "causal_clustering.raft_in_queue_max_bytes", BYTES, ByteUnit.gibiBytes( 2 ) ).build();

    @Internal
    @Description( "Largest batch processed by RAFT in number of entries" )
    public static final Setting<Integer> raft_in_queue_max_batch =
            newBuilder( "causal_clustering.raft_in_queue_max_batch", INT, 128 ).build();

    @Description( "Largest batch processed by RAFT in bytes" )
    public static final Setting<Long> raft_in_queue_max_batch_bytes =
            newBuilder( "causal_clustering.raft_in_queue_max_batch_bytes", BYTES, ByteUnit.mebiBytes( 8 ) ).build();

    @Description( "Minimum number of Core machines initially required to form a cluster. " +
            "The cluster will form when at least this many Core members have discovered each other." )
    public static final Setting<Integer> minimum_core_cluster_size_at_formation =
            newBuilder( "causal_clustering.minimum_core_cluster_size_at_formation", INT, 3 ).addConstraint( min( 2 ) ).build();

    @Description( "The minimum size of the dynamically adjusted voting set (which only core members may be a part of). " +
            "Adjustments to the voting set happen automatically as the availability of core members changes, " +
            "due to explicit operations such as starting or stopping a member, " +
            "or unintended issues such as network partitions. " +
            "Note that this dynamic scaling of the voting set is generally desirable as under some circumstances " +
            "it can increase the number of instance failures which may be tolerated. " +
            "A majority of the voting set must be available before voting in or out members." )
    public static final Setting<Integer> minimum_core_cluster_size_at_runtime =
            newBuilder( "causal_clustering.minimum_core_cluster_size_at_runtime", INT, 3 ).addConstraint( min( 2 ) ).build();

    public static final int DEFAULT_DISCOVERY_PORT = 5000;
    public static final int DEFAULT_TRANSACTION_PORT = 6000;
    public static final int DEFAULT_RAFT_PORT = 7000;

    @Description( "Network interface and port for the transaction shipping server to listen on. Please note that it is also possible to run the backup " +
            "client against this port so always limit access to it via the firewall and configure an ssl policy." )
    public static final Setting<SocketAddress> transaction_listen_address =
            newBuilder( "causal_clustering.transaction_listen_address", SOCKET_ADDRESS, new SocketAddress( DEFAULT_TRANSACTION_PORT ) )
                    .setDependency( default_listen_address )
                    .build();

    @Description( "Advertised hostname/IP address and port for the transaction shipping server." )
    public static final Setting<SocketAddress> transaction_advertised_address =
            newBuilder( "causal_clustering.transaction_advertised_address", SOCKET_ADDRESS, new SocketAddress( DEFAULT_TRANSACTION_PORT ) )
                    .setDependency( default_advertised_address )
                    .build();

    @Description( "Network interface and port for the RAFT server to listen on." )
    public static final Setting<SocketAddress> raft_listen_address =
            newBuilder( "causal_clustering.raft_listen_address", SOCKET_ADDRESS, new SocketAddress( DEFAULT_RAFT_PORT ) )
                    .setDependency( default_listen_address )
                    .build();

    @Description( "Advertised hostname/IP address and port for the RAFT server." )
    public static final Setting<SocketAddress> raft_advertised_address =
            newBuilder( "causal_clustering.raft_advertised_address", SOCKET_ADDRESS, new SocketAddress( DEFAULT_RAFT_PORT ) )
                    .setDependency( default_advertised_address )
                    .build();

    @Description( "Host and port to bind the cluster member discovery management communication." )
    public static final Setting<SocketAddress> discovery_listen_address =
            newBuilder( "causal_clustering.discovery_listen_address", SOCKET_ADDRESS, new SocketAddress( DEFAULT_DISCOVERY_PORT ) )
                    .setDependency( default_listen_address )
                    .build();

    @Description( "Advertised cluster member discovery management communication." )
    public static final Setting<SocketAddress> discovery_advertised_address =
            newBuilder( "causal_clustering.discovery_advertised_address", SOCKET_ADDRESS, new SocketAddress( DEFAULT_DISCOVERY_PORT ) )
                    .setDependency( default_advertised_address )
                    .build();

    @Description( "A comma-separated list of other members of the cluster to join." )
    public static final Setting<List<SocketAddress>> initial_discovery_members =
            newBuilder( "causal_clustering.initial_discovery_members", listOf( SOCKET_ADDRESS ), null ).build();

    @Internal
    @Description( "Use native transport if available. Epoll for Linux or Kqueue for MacOS/BSD. If this setting is set to false, or if native transport is " +
            "not available, Nio transport will be used." )
    public static final Setting<Boolean> use_native_transport =
            newBuilder( "causal_clustering.use_native_transport", BOOL,  true  ).build();

    @Description( "Type of in-flight cache." )
    public static final Setting<InFlightCacheFactory.Type> in_flight_cache_type =
            newBuilder( "causal_clustering.in_flight_cache.type", ofEnum( InFlightCacheFactory.Type.class ), InFlightCacheFactory.Type.CONSECUTIVE ).build();

    @Description( "The maximum number of entries in the in-flight cache." )
    public static final Setting<Integer> in_flight_cache_max_entries =
            newBuilder( "causal_clustering.in_flight_cache.max_entries", INT, 1024 ).build();

    @Description( "The maximum number of bytes in the in-flight cache." )
    public static final Setting<Long> in_flight_cache_max_bytes =
            newBuilder( "causal_clustering.in_flight_cache.max_bytes", BYTES, ByteUnit.gibiBytes( 2 ) ).build();

    @Description( "Address for Kubernetes API" )
    public static final Setting<SocketAddress> kubernetes_address =
            newBuilder( "causal_clustering.kubernetes.address", SOCKET_ADDRESS, new SocketAddress( "kubernetes.default.svc", 443 ) ).build();

    @Description( "File location of token for Kubernetes API" )
    public static final Setting<Path> kubernetes_token =
            pathUnixAbsolute( "causal_clustering.kubernetes.token", "/var/run/secrets/kubernetes.io/serviceaccount/token" );

    @Description( "File location of namespace for Kubernetes API" )
    public static final Setting<Path> kubernetes_namespace =
            pathUnixAbsolute( "causal_clustering.kubernetes.namespace", "/var/run/secrets/kubernetes.io/serviceaccount/namespace" );

    @Description( "File location of CA certificate for Kubernetes API" )
    public static final Setting<Path> kubernetes_ca_crt =
            pathUnixAbsolute( "causal_clustering.kubernetes.ca_crt", "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt" );

    /**
     * Creates absolute path on the first filesystem root. This will be `/` on Unix but arbitrary on Windows.
     * If filesystem roots cannot be listed then `//` will be used - this will be resolved to `/` on Unix and `\\` (a UNC network path) on Windows.
     * An absolute path is always needed for validation, even though we only care about a path on Linux.
     */
    private static Setting<Path> pathUnixAbsolute( String name, String path )
    {
        File[] roots = File.listRoots();
        Path root = roots.length > 0 ? roots[0].toPath() : Paths.get( "//" );
        return newBuilder( name, PATH,  root.resolve( path  ) ).build();
    }

    @Description( "LabelSelector for Kubernetes API" )
    public static final Setting<String> kubernetes_label_selector =
            newBuilder( "causal_clustering.kubernetes.label_selector", STRING, null ).build();

    @Description( "Service port name for discovery for Kubernetes API" )
    public static final Setting<String> kubernetes_service_port_name =
            newBuilder( "causal_clustering.kubernetes.service_port_name", STRING, null ).build();

    @Internal
    @Description( "Configures the time taken attempting to publish a cluster id to the discovery service before potentially retrying." )
    public static final Setting<Duration> raft_id_publish_timeout =
            newBuilder( "causal_clustering.cluster_id_publish_timeout", DURATION, ofSeconds( 15 ) ).build();

    @Internal
    @Description( "The polling interval when attempting to resolve initial_discovery_members from DNS and SRV records." )
    public static final Setting<Duration> discovery_resolution_retry_interval =
            newBuilder( "causal_clustering.discovery_resolution_retry_interval", DURATION, ofSeconds( 5 ) ).build();

    @Internal
    @Description( "Configures the time after which we give up trying to resolve a DNS/SRV record into a list of initial discovery members." )
    public static final Setting<Duration> discovery_resolution_timeout =
            newBuilder( "causal_clustering.discovery_resolution_timeout", DURATION, ofMinutes( 5 ) ).build();

    @Description( "Configure the discovery type used for cluster name resolution" )
    public static final Setting<DiscoveryType> discovery_type =
            newBuilder( "causal_clustering.discovery_type", ofEnum( DiscoveryType.class ), DiscoveryType.LIST ).build();

    @Description( "The level of middleware logging" )
    public static final Setting<Level> middleware_logging_level =
            newBuilder( "causal_clustering.middleware.logging.level", ofEnum( Level.class ), Level.WARN ).build();

    @Internal
    @Description( "External config file for Akka" )
    public static final Setting<Path> middleware_akka_external_config =
            newBuilder( "causal_clustering.middleware.akka.external_config", PATH, null ).build();

    @Internal
    @Description( "Parallelism level of default dispatcher used by Akka based cluster topology discovery, including cluster, replicator, and discovery actors" )
    public static final Setting<Integer> middleware_akka_default_parallelism_level =
            newBuilder( "causal_clustering.middleware.akka.default-parallelism", INT, 4 ).build();

    @Internal
    @Description( "Parallelism level of dispatcher used for communication from Akka based cluster topology discovery " )
    public static final Setting<Integer> middleware_akka_sink_parallelism_level =
            newBuilder( "causal_clustering.middleware.akka.sink-parallelism", INT, 2 ).build();

    @Internal
    @Description( "Timeout for Akka socket binding" )
    public static final Setting<Duration> akka_bind_timeout =
            newBuilder( "causal_clustering.middleware.akka.bind-timeout", DURATION,  ofSeconds( 10 ) ).build();

    @Internal
    @Description( "Timeout for Akka connection" )
    public static final Setting<Duration> akka_connection_timeout =
            newBuilder( "causal_clustering.middleware.akka.connection-timeout", DURATION,  ofSeconds( 10 ) ).build();

    @Internal
    @Description( "Timeout for Akka handshake" )
    public static final Setting<Duration> akka_handshake_timeout =
            newBuilder( "causal_clustering.middleware.akka.handshake-timeout", DURATION,  ofSeconds( 30 ) ).build();

    /*
        Begin akka failure detector
        setting descriptions copied from reference.conf in akka-cluster
     */
    @Internal
    @Description( "Akka cluster phi accrual failure detector. " +
            "How often keep-alive heartbeat messages should be sent to each connection." )
    public static final Setting<Duration> akka_failure_detector_heartbeat_interval =
            newBuilder( "causal_clustering.middleware.akka.failure_detector.heartbeat_interval", DURATION, ofSeconds( 1 ) ).build();

    @Internal
    @Description( "Akka cluster phi accrual failure detector. " +
            "Defines the failure detector threshold. " +
            "A low threshold is prone to generate many wrong suspicions but ensures " +
            "a quick detection in the event of a real crash. Conversely, a high " +
            "threshold generates fewer mistakes but needs more time to detect actual crashes." )
    public static final Setting<Double> akka_failure_detector_threshold =
            newBuilder( "causal_clustering.middleware.akka.failure_detector.threshold", DOUBLE, 10.0 ).build();

    @Internal
    @Description( "Akka cluster phi accrual failure detector. " +
            "Number of the samples of inter-heartbeat arrival times to adaptively " +
            "calculate the failure timeout for connections." )
    public static final Setting<Integer> akka_failure_detector_max_sample_size =
            newBuilder( "causal_clustering.middleware.akka.failure_detector.max_sample_size", INT, 1000 ).build();

    @Internal
    @Description( "Akka cluster phi accrual failure detector. " +
            "Minimum standard deviation to use for the normal distribution in " +
            "AccrualFailureDetector. Too low standard deviation might result in " +
            "too much sensitivity for sudden, but normal, deviations in heartbeat inter arrival times." )
    public static final Setting<Duration> akka_failure_detector_min_std_deviation =
            newBuilder( "causal_clustering.middleware.akka.failure_detector.min_std_deviation", DURATION, Duration.ofMillis( 100 ) ).build();

    @Internal
    @Description( "Akka cluster phi accrual failure detector. " +
            "Number of potentially lost/delayed heartbeats that will be " +
            "accepted before considering it to be an anomaly. " +
            "This margin is important to be able to survive sudden, occasional, " +
            "pauses in heartbeat arrivals, due to for example garbage collect or network drop." )
    public static final Setting<Duration> akka_failure_detector_acceptable_heartbeat_pause =
            newBuilder( "causal_clustering.middleware.akka.failure_detector.acceptable_heartbeat_pause", DURATION, ofSeconds( 4 ) ).build();

    @Internal
    @Description( "Akka cluster phi accrual failure detector. " +
            "Number of member nodes that each member will send heartbeat messages to, " +
            "i.e. each node will be monitored by this number of other nodes." )
    public static final Setting<Integer> akka_failure_detector_monitored_by_nr_of_members =
            newBuilder( "causal_clustering.middleware.akka.failure_detector.monitored_by_nr_of_members", INT, 5 ).build();

    @Internal
    @Description( "Akka cluster phi accrual failure detector. " +
            "After the heartbeat request has been sent the first failure detection " +
            "will start after this period, even though no heartbeat message has been received." )
    public static final Setting<Duration> akka_failure_detector_expected_response_after =
            newBuilder( "causal_clustering.middleware.akka.failure_detector.expected_response_after", DURATION, ofSeconds( 1 ) ).build();
    /*
        End akka failure detector
     */

    @Description( "The maximum file size before the storage file is rotated (in unit of entries)" )
    public static final Setting<Integer> last_flushed_state_size =
            newBuilder( "causal_clustering.last_applied_state_size", INT, 1000 ).build();

    @Description( "The maximum file size before the membership state file is rotated (in unit of entries)" )
    public static final Setting<Integer> raft_membership_state_size =
            newBuilder( "causal_clustering.raft_membership_state_size", INT, 1000 ).build();

    @Description( "The maximum file size before the vote state file is rotated (in unit of entries)" )
    public static final Setting<Integer> vote_state_size =
            newBuilder( "causal_clustering.raft_vote_state_size", INT, 1000 ).build();

    @Description( "The maximum file size before the term state file is rotated (in unit of entries)" )
    public static final Setting<Integer> term_state_size =
            newBuilder( "causal_clustering.raft_term_state_size", INT, 1000 ).build();

    @Description( "The maximum file size before the global session tracker state file is rotated (in unit of entries)" )
    public static final Setting<Integer> global_session_tracker_state_size =
            newBuilder( "causal_clustering.global_session_tracker_state_size", INT, 1000 ).build();

    @Description( "The maximum file size before the replicated lease state file is rotated (in unit of entries)" )
    public static final Setting<Integer> replicated_lease_state_size =
            newBuilder( "causal_clustering.replicated_lease_state_size", INT, 1000 ).build();

    @Description( "The initial timeout until replication is retried. The timeout will increase exponentially." )
    public static final Setting<Duration> replication_retry_timeout_base =
            newBuilder( "causal_clustering.replication_retry_timeout_base", DURATION, ofSeconds( 10 ) ).build();

    @Description( "The upper limit for the exponentially incremented retry timeout." )
    public static final Setting<Duration> replication_retry_timeout_limit =
            newBuilder( "causal_clustering.replication_retry_timeout_limit", DURATION, ofSeconds( 60 ) ).build();

    @Description( "The duration for which the replicator will await a new leader." )
    public static final Setting<Duration> replication_leader_await_timeout =
            newBuilder( "causal_clustering.replication_leader_await_timeout", DURATION, ofSeconds( 10 ) ).build();

    @Description( "The number of operations to be processed before the state machines flush to disk" )
    public static final Setting<Integer> state_machine_flush_window_size =
            newBuilder( "causal_clustering.state_machine_flush_window_size", INT, 4096 ).build();

    @Description( "The maximum number of operations to be batched during applications of operations in the state machines" )
    public static final Setting<Integer> state_machine_apply_max_batch_size =
            newBuilder( "causal_clustering.state_machine_apply_max_batch_size", INT, 16 ).build();

    @Description( "RAFT log pruning strategy" )
    public static final Setting<String> raft_log_pruning_strategy =
            newBuilder( "causal_clustering.raft_log_prune_strategy", STRING,  "1g size"  ).build();

    @Description( "RAFT log implementation" )
    public static final Setting<String> raft_log_implementation =
            newBuilder( "causal_clustering.raft_log_implementation", STRING, "SEGMENTED" ).build();

    @Description( "RAFT log rotation size" )
    public static final Setting<Long> raft_log_rotation_size =
            newBuilder( "causal_clustering.raft_log_rotation_size", BYTES, ByteUnit.mebiBytes( 250 ) ).addConstraint( min( 1024L ) ).build();

    @Description( "RAFT log reader pool size" )
    public static final Setting<Integer> raft_log_reader_pool_size =
            newBuilder( "causal_clustering.raft_log_reader_pool_size", INT, 8 ).build();

    @Description( "RAFT log pruning frequency" )
    public static final Setting<Duration> raft_log_pruning_frequency =
            newBuilder( "causal_clustering.raft_log_pruning_frequency", DURATION, ofMinutes( 10 ) ).build();

    @Description( "Enable or disable the dump of all network messages pertaining to the RAFT protocol" )
    @Internal
    public static final Setting<Boolean> raft_messages_log_enable =
            newBuilder( "causal_clustering.raft_messages_log_enable", BOOL,  false  ).build();

    @Description( "Path to RAFT messages log." )
    @Internal
    public static final Setting<Path> raft_messages_log_path =
            newBuilder( "causal_clustering.raft_messages_log_path", PATH, Path.of( "raft-messages.log" ) )
                    .setDependency( GraphDatabaseSettings.logs_directory ).immutable().build();

    @Description( "Interval of pulling updates from cores." )
    public static final Setting<Duration> pull_interval = newBuilder( "causal_clustering.pull_interval", DURATION, ofSeconds( 1 ) ).build();

    @Description( "The catch up protocol times out if the given duration elapses with no network activity. " +
            "Every message received by the client from the server extends the time out duration." )
    public static final Setting<Duration> catch_up_client_inactivity_timeout =
            newBuilder( "causal_clustering.catch_up_client_inactivity_timeout", DURATION, ofMinutes( 10 ) ).build();

    @Description( "Maximum retry time per request during store copy. Regular store files and indexes are downloaded in separate requests during store copy." +
            " This configures the maximum time failed requests are allowed to resend. " )
    public static final Setting<Duration> store_copy_max_retry_time_per_request =
            newBuilder( "causal_clustering.store_copy_max_retry_time_per_request", DURATION, ofMinutes( 20 ) ).build();

    @Description( "Maximum backoff timeout for store copy requests" )
    @Internal
    public static final Setting<Duration> store_copy_backoff_max_wait =
            newBuilder( "causal_clustering.store_copy_backoff_max_wait", DURATION, ofSeconds( 5 ) ).build();

    @Description( "Store copy chunk size" )
    public static final Setting<Integer> store_copy_chunk_size =
            newBuilder( "causal_clustering.store_copy_chunk_size", INT, (int) kibiBytes( 32 ) )
                    .addConstraint( range( (int) kibiBytes( 4 ), (int) mebiBytes( 1 ) ) )
                    .build();

    @Description( "Throttle limit for logging unknown cluster member address" )
    public static final Setting<Duration> unknown_address_logging_throttle =
            newBuilder( "causal_clustering.unknown_address_logging_throttle", DURATION, Duration.ofMillis( 10000 ) ).build();

    @Description( "Maximum transaction batch size for read replicas when applying transactions pulled from core " +
            "servers." )
    @Internal
    public static final Setting<Integer> read_replica_transaction_applier_batch_size =
            newBuilder( "causal_clustering.read_replica_transaction_applier_batch_size", INT, 64 ).build();

    @Description( "Configure if the `dbms.routing.getRoutingTable()` procedure should include followers as read " +
            "endpoints or return only read replicas. Note: if there are no read replicas in the cluster, followers " +
            "are returned as read end points regardless the value of this setting. Defaults to true so that followers " +
            "are available for read-only queries in a typical heterogeneous setup." )
    public static final Setting<Boolean> cluster_allow_reads_on_followers =
            newBuilder( "causal_clustering.cluster_allow_reads_on_followers", BOOL,  true  ).build();

    @Description( "Time between scanning the cluster to refresh current server's view of topology" )
    public static final Setting<Duration> cluster_topology_refresh =
            newBuilder( "causal_clustering.cluster_topology_refresh", DURATION, ofSeconds( 5 ) ).addConstraint( min( ofSeconds( 1 ) ) ).build();

    @Description( "An ordered list in descending preference of the strategy which read replicas use to choose " +
            "the upstream server from which to pull transactional updates." )
    public static final Setting<List<String>> upstream_selection_strategy =
            newBuilder( "causal_clustering.upstream_selection_strategy", listOf( STRING ), List.of("default") ).build();

    @Description( "Configuration of a user-defined upstream selection strategy. " +
            "The user-defined strategy is used if the list of strategies (`causal_clustering.upstream_selection_strategy`) " +
            "includes the value `user_defined`. " )
    public static final Setting<String> user_defined_upstream_selection_strategy =
            newBuilder( "causal_clustering.user_defined_upstream_strategy", STRING, "" ).build();

    @Description( "Comma separated list of groups to be used by the connect-randomly-to-server-group selection strategy. " +
            "The connect-randomly-to-server-group strategy is used if the list of strategies (`causal_clustering.upstream_selection_strategy`) " +
            "includes the value `connect-randomly-to-server-group`. " )
    public static final Setting<List<ServerGroupName>> connect_randomly_to_server_group_strategy =
            newBuilder( "causal_clustering.connect-randomly-to-server-group", listOf( SERVER_GROUP_NAME ), emptyList() ).build();

    @Description( "A list of group names for the server used when configuring load balancing and replication policies." )
    public static final Setting<List<ServerGroupName>> server_groups =
            newBuilder( "causal_clustering.server_groups", listOf( SERVER_GROUP_NAME ), emptyList() ).build();

    @Description( "A list of group names where leadership should be prioritised. This does not guarantee leadership on these grups at all times, but" +
                  " the cluster will attempt to transfer leadership to these groups when possible." )
    public static final Setting<List<String>> leadership_priority_groups =
            newBuilder( "causal_clustering.leadership_priority_groups", listOf( STRING ), emptyList() ).build();

    @Description( "The load balancing plugin to use." )
    public static final Setting<String> load_balancing_plugin =
            newBuilder( "causal_clustering.load_balancing.plugin", STRING, "server_policies" ).build();

    @Description( "Time out for protocol negotiation handshake" )
    public static final Setting<Duration> handshake_timeout =
            newBuilder( "causal_clustering.handshake_timeout", DURATION, ofSeconds( 20 ) ).build();

    @Description( "Enables shuffling of the returned load balancing result." )
    public static final Setting<Boolean> load_balancing_shuffle =
            newBuilder( "causal_clustering.load_balancing.shuffle", BOOL,  true  ).build();

    @Description( "Require authorization for access to the Causal Clustering status endpoints." )
    public static final Setting<Boolean> status_auth_enabled =
            newBuilder( "dbms.security.causal_clustering_status_auth_enabled", BOOL,  true  ).build();

    @Description( "Sampling window for throughput estimate reported in the status endpoint." )
    public static final Setting<Duration> status_throughput_window =
            newBuilder( "causal_clustering.status_throughput_window", DURATION, ofSeconds( 5 ) )
                    .addConstraint( range( ofSeconds( 1 ), ofMinutes( 5 ) ) )
                    .build();

    @Description( "Enable multi-data center features. Requires appropriate licensing." )
    public static final Setting<Boolean> multi_dc_license =
            newBuilder( "causal_clustering.multi_dc_license", BOOL,  false  ).build();

    private static final SettingValueParser<ApplicationProtocolVersion> APP_PROTOCOL_VER = new SettingValueParser<>()
    {

        @Override
        public ApplicationProtocolVersion parse( String value )
        {
            return ApplicationProtocolVersion.parse( value );
        }

        @Override
        public String getDescription()
        {
            return "an application protocol version";
        }

        @Override
        public Class<ApplicationProtocolVersion> getType()
        {
            return ApplicationProtocolVersion.class;
        }
    };

    @Description( "Raft protocol implementation versions that this instance will allow in negotiation as a comma-separated list. " +
            "Order is not relevant: the greatest value will be preferred. An empty list will allow all supported versions. " +
            "Example value: \"1.0, 1.3, 2.0, 2.1\"" )
    public static final Setting<List<ApplicationProtocolVersion>> raft_implementations =
            newBuilder( "causal_clustering.protocol_implementations.raft", listOf( APP_PROTOCOL_VER ), emptyList() ).build();

    @Description( "Catchup protocol implementation versions that this instance will allow in negotiation as a comma-separated list. " +
            "Order is not relevant: the greatest value will be preferred. An empty list will allow all supported versions. " +
            "Example value: \"1.1, 1.2, 2.1, 2.2\"" )
    public static final Setting<List<ApplicationProtocolVersion>> catchup_implementations =
            newBuilder( "causal_clustering.protocol_implementations.catchup", listOf( APP_PROTOCOL_VER ), emptyList() ).build();

    @Description( "Network compression algorithms that this instance will allow in negotiation as a comma-separated list." +
            " Listed in descending order of preference for incoming connections. An empty list implies no compression." +
            " For outgoing connections this merely specifies the allowed set of algorithms and the preference of the " +
            " remote peer will be used for making the decision." +
            " Allowable values: " + ModifierProtocols.ALLOWED_VALUES_STRING )
    public static final Setting<List<String>> compression_implementations =
            newBuilder( "causal_clustering.protocol_implementations.compression", listOf( STRING ), emptyList() ).build();

    @Internal
    public static final Setting<Boolean> inbound_connection_initialization_logging_enabled =
            newBuilder( "unsupported.causal_clustering.inbound_connection_initialization_logging_enabled", BOOL, true ).dynamic().build();
}
