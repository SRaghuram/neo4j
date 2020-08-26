/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import java.nio.file.Path;
import java.time.Duration;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

import static com.neo4j.configuration.CausalClusterSettingConstraints.validateMiddlewareConfig;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.neo4j.configuration.SettingConstraints.lessThanOrEqual;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.DOUBLE;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.PATH;

@ServiceProvider
public class CausalClusteringInternalSettings implements SettingsDeclaration
{
    public static final String TEMP_STORE_COPY_DIRECTORY_NAME = "temp-copy";
    public static final String TEMP_BOOTSTRAP_DIRECTORY_NAME = "temp-bootstrap";
    public static final String TEMP_SAVE_DIRECTORY_NAME = "temp-save";

    @Internal
    @Description( "The time limit within which a leadership transfer request should be completed, otherwise the leader will resume accepting writes." )
    public static final Setting<Duration> leader_transfer_timeout =
            newBuilder( "causal_clustering.leader_transfer_timeout", DURATION, ofSeconds( 3 ) ).build();

    @Internal
    @Description( "The frequency with which a leader will try and transfer leadership to another member" )
    public static final Setting<Duration> leader_transfer_interval =
            newBuilder( "causal_clustering.leader_transfer_interval", DURATION, ofSeconds( 15 ) ).build();

    @Internal
    @Description( "The amount of time we should wait before repeating an attempt to transfer the leadership of a given database to a member after" +
            " that member rejects a previous transfer." )
    public static final Setting<Duration> leader_transfer_member_backoff =
            newBuilder( "causal_clustering.leader_transfer_member_backoff", DURATION, ofSeconds( 30 ) ).build();

    @Internal
    @Description( "Configures the time after which we retry binding to a cluster. Only applies to Akka discovery. " +
            "A discovery type of DNS/SRV/K8S will be queried again on retry." )
    public static final Setting<Duration> cluster_binding_retry_timeout =
            newBuilder( "causal_clustering.cluster_binding_retry_timeout", DURATION, ofMinutes( 1 ) ).build();

    @Internal
    @Description( "Maximum number of entries in the RAFT in-queue" )
    public static final Setting<Integer> raft_in_queue_size = newBuilder( "causal_clustering.raft_in_queue_size", INT, 1024 ).build();

    @Internal
    @Description( "Largest batch processed by RAFT in number of entries" )
    public static final Setting<Integer> raft_in_queue_max_batch = newBuilder( "causal_clustering.raft_in_queue_max_batch", INT, 128 ).build();

    @Internal
    @Description( "Use native transport if available. Epoll for Linux or Kqueue for MacOS/BSD. If this setting is set to false, or if native transport is " +
            "not available, Nio transport will be used." )
    public static final Setting<Boolean> use_native_transport =
            newBuilder( "causal_clustering.use_native_transport", BOOL,  true  ).build();

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

    @Internal
    @Description( "External config file for Akka" )
    public static final Setting<Path> middleware_akka_external_config =
            newBuilder( "causal_clustering.middleware.akka.external_config", PATH, null )
                    .addConstraint( validateMiddlewareConfig() ).build();

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

    @Internal
    @Description( "Time that seed nodes will spend trying to find an existing cluster before forming a new cluster" )
    public static final Setting<Duration> middleware_akka_seed_node_timeout =
            newBuilder( "causal_clustering.middleware.akka.cluster.seed_node_timeout", DURATION, Duration.ofSeconds( 30 ) )
                    .addConstraint( lessThanOrEqual( Duration::toMillis,
                                                     cluster_binding_retry_timeout,
                                                     clusterBindingTimeout -> clusterBindingTimeout / 2,
                                                     "divided by 2" ) )
                    .build();

    @Internal
    @Description( "Time that seed nodes will spend trying to find an existing cluster before forming a new cluster, when Neo4j is started for the first time" )
    public static final Setting<Duration> middleware_akka_seed_node_timeout_on_first_start =
            newBuilder( "causal_clustering.middleware.akka.cluster.seed_node_timeout_on_first_start", DURATION, Duration.ofSeconds( 3 ) )
                    .addConstraint( lessThanOrEqual( Duration::toMillis, middleware_akka_seed_node_timeout ) )
                    .build();

    /*
        Begin akka failure detector
        setting descriptions copied from reference.conf in akka-cluster
    */
    @Internal
    @Description( "Akka cluster phi accrual failure detector. How often keep-alive heartbeat messages should be sent to each connection." )
    public static final Setting<Duration> akka_failure_detector_heartbeat_interval =
            newBuilder( "causal_clustering.middleware.akka.failure_detector.heartbeat_interval", DURATION, ofSeconds( 1 ) ).build();

    @Internal
    @Description( "Akka cluster phi accrual failure detector. Defines the failure detector threshold. A low threshold is prone to generate many wrong " +
            "suspicions but ensures a quick detection in the event of a real crash. Conversely, a high threshold generates fewer mistakes but needs more " +
            "time to detect actual crashes." )
    public static final Setting<Double> akka_failure_detector_threshold =
            newBuilder( "causal_clustering.middleware.akka.failure_detector.threshold", DOUBLE, 10.0 ).build();

    @Internal
    @Description( "Akka cluster phi accrual failure detector. Number of the samples of inter-heartbeat arrival times to adaptively calculate the failure " +
            "timeout for connections." )
    public static final Setting<Integer> akka_failure_detector_max_sample_size =
            newBuilder( "causal_clustering.middleware.akka.failure_detector.max_sample_size", INT, 1000 ).build();

    @Internal
    @Description( "Akka cluster phi accrual failure detector. Minimum standard deviation to use for the normal distribution in AccrualFailureDetector. " +
            "Too low standard deviation might result in too much sensitivity for sudden, but normal, deviations in heartbeat inter arrival times." )
    public static final Setting<Duration> akka_failure_detector_min_std_deviation =
            newBuilder( "causal_clustering.middleware.akka.failure_detector.min_std_deviation", DURATION, Duration.ofMillis( 100 ) ).build();

    @Internal
    @Description( "Akka cluster phi accrual failure detector. Number of potentially lost/delayed heartbeats that will be accepted before considering it " +
            "to be an anomaly. This margin is important to be able to survive sudden, occasional, pauses in heartbeat arrivals, due to for example garbage " +
            "collect or network drop." )
    public static final Setting<Duration> akka_failure_detector_acceptable_heartbeat_pause =
            newBuilder( "causal_clustering.middleware.akka.failure_detector.acceptable_heartbeat_pause", DURATION, ofSeconds( 4 ) ).build();

    @Internal
    @Description( "Akka cluster phi accrual failure detector. Number of member nodes that each member will send heartbeat messages to, i.e. each node " +
            "will be monitored by this number of other nodes." )
    public static final Setting<Integer> akka_failure_detector_monitored_by_nr_of_members =
            newBuilder( "causal_clustering.middleware.akka.failure_detector.monitored_by_nr_of_members", INT, 5 ).build();

    @Internal
    @Description( "Akka cluster phi accrual failure detector. After the heartbeat request has been sent the first failure detection will start after this " +
            "period, even though no heartbeat message has been received." )
    public static final Setting<Duration> akka_failure_detector_expected_response_after =
            newBuilder( "causal_clustering.middleware.akka.failure_detector.expected_response_after", DURATION, ofSeconds( 1 ) ).build();
    /*
    End akka failure detector
    */

    @Internal
    @Description( "Enable or disable the dump of all network messages pertaining to the RAFT protocol" )
    public static final Setting<Boolean> raft_messages_log_enable = newBuilder( "causal_clustering.raft_messages_log_enable", BOOL, false ).build();

    @Internal
    @Description( "Path to RAFT messages log." )
    public static final Setting<Path> raft_messages_log_path = newBuilder( "causal_clustering.raft_messages_log_path", PATH, Path.of( "raft-messages.log" ) )
            .setDependency( GraphDatabaseSettings.logs_directory )
            .immutable()
            .build();

    @Internal
    @Description( "Maximum backoff timeout for store copy requests" )
    public static final Setting<Duration> store_copy_backoff_max_wait =
            newBuilder( "causal_clustering.store_copy_backoff_max_wait", DURATION, ofSeconds( 5 ) ).build();

    @Internal
    @Description( "Maximum transaction batch size for read replicas when applying transactions pulled from core servers." )
    public static final Setting<Integer> read_replica_transaction_applier_batch_size =
            newBuilder( "causal_clustering.read_replica_transaction_applier_batch_size", INT, 64 ).build();

    @Internal
    public static final Setting<Boolean> inbound_connection_initialization_logging_enabled =
            newBuilder( "unsupported.causal_clustering.inbound_connection_initialization_logging_enabled", BOOL, true ).dynamic().build();

    @Internal
    public static final Setting<Boolean> experimental_raft_protocol =
            newBuilder( "unsupported.causal_clustering.experimental_raft_protocol_enabled", BOOL, false ).build();

    @Internal
    public static final Setting<Boolean> experimental_catchup_protocol =
            newBuilder( "unsupported.causal_clustering.experimental_catchup_protocol_enabled", BOOL, false ).build();

    @Internal
    @Description( "Maximum timeout for cluster status request execution" )
    public static final Setting<Duration> cluster_status_request_maximum_wait =
            newBuilder( "unsupported.causal_clustering.cluster_status_request_maximum_wait", DURATION, ofSeconds( 5 ) ).build();
}
