/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.CausalClusteringTestHelpers;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.readreplica.CatchupProcessManager;
import com.neo4j.dbms.EnterpriseOperatorState;
import com.neo4j.security.SecurityHelpers;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import com.neo4j.test.driver.DriverExtension;
import com.neo4j.test.driver.DriverFactory;
import com.neo4j.test.driver.WaitResponse;
import com.neo4j.test.driver.WaitResponseStates;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.databaseStates;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.dropDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.startDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.stopDatabase;
import static com.neo4j.configuration.EnterpriseEditionSettings.max_number_of_databases;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

@DriverExtension
@ClusterExtension
public class WaitProcedureIT
{
    private static final Duration DEFAULT_PROCEDURE_TIMEOUT = Duration.ofSeconds( 10 );

    @Inject
    private ClusterFactory clusterFactory;
    @Inject
    private DriverFactory driverFactory;

    private Cluster cluster;
    private NamedDatabaseId databaseId;
    private Driver systemDbDriver;

    @BeforeAll
    void setUp() throws Exception
    {
        cluster = clusterFactory.createCluster( ClusterConfig.clusterConfig()
                .withSharedReadReplicaParam( max_number_of_databases, "3" )
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 1 ) );
        cluster.start();
        var databaseName = "foo";
        CausalClusteringTestHelpers.createDatabase( databaseName, cluster );
        var leaderFoo = cluster.awaitLeader( "foo" );
        databaseId = leaderFoo.databaseId( "foo" );
        systemDbDriver = driverFactory.graphDatabaseDriver( cluster );
    }

    @Test
    void shouldBeIncompleteIfSystemDbUpdateIsNotObserved() throws TimeoutException
    {
        var replica = cluster.findAnyReadReplica();
        replica.resolveDependency( "system", CatchupProcessManager.class ).pauseCatchupProcess();
        try
        {
            SecurityHelpers.newUser( systemDbDriver, "some", "person" );
            var lastCommitTxId =
                    cluster.awaitLeader( "system" ).resolveDependency( "system", TransactionIdStore.class ).getLastCommittedTransactionId();

            var result = runProcedure( systemDbDriver, lastCommitTxId, databaseId, Duration.ofSeconds( 2 ) );

            var coreStates = result.stream()
                    .filter( not( response -> response.address().equals( replica.boltAdvertisedAddress().toString() ) ) )
                    .collect( toList() );

            var readReplicaStates = result.stream()
                    .filter( response -> response.address().equals( replica.boltAdvertisedAddress().toString() ) )
                    .collect( toList() );

            assertThat( coreStates ).hasSize( 3 );
            assertThat( readReplicaStates ).hasSize( 1 )
                    .allMatch( response -> response.state() == WaitResponseStates.Incomplete )
                    .allMatch( response -> !response.success() );
        }
        finally
        {
            replica.resolveDependency( "system", CatchupProcessManager.class ).resumeCatchupProcess();
        }
    }

    @Test
    void shouldReactToTopologyServiceChanges() throws TimeoutException, ExecutionException, InterruptedException
    {
        var replica = cluster.newReadReplica();
        replica.start();
        // New read replicas not pulling any updates
        replica.resolveDependency( "system", CatchupProcessManager.class ).pauseCatchupProcess();
        SecurityHelpers.newUser( systemDbDriver, "other", "person" );
        var lastCommitTxId =
                cluster.awaitLeader( "system" ).resolveDependency( "system", TransactionIdStore.class ).getLastCommittedTransactionId();

        var asyncProcedure = CompletableFuture.supplyAsync( () -> runProcedure( systemDbDriver, lastCommitTxId, databaseId, Duration.ofMinutes( 5 ) ) );

        var temporaryReplicaServerId = replica.boltAdvertisedAddress();
        cluster.removeReadReplica( replica );

        var result = asyncProcedure.get();

        assertThat( result )
                .hasSize( 4 )
                .allMatch( response -> response.state() == WaitResponseStates.CaughtUp )
                .allMatch( WaitResponse::success )
                .noneMatch( waitResponse -> temporaryReplicaServerId.toString().equals( waitResponse.address() ) );
    }

    @Test
    void shouldProjectReconciliationError() throws Exception
    {
        createDatabase( "bar", cluster );
        var lastCommitTxId =
                cluster.awaitLeader( "system" ).resolveDependency( "system", TransactionIdStore.class ).getLastCommittedTransactionId();

        var newDatabaseId = cluster.awaitLeader( "bar" ).databaseId( "bar" );

        var responses = runProcedure( systemDbDriver, lastCommitTxId, newDatabaseId, DEFAULT_PROCEDURE_TIMEOUT );

        // RR is configured to not allow more than 3 databases
        var constrainedServer = cluster.findAnyReadReplica().boltAdvertisedAddress();
        var failed = responses.stream().filter( response -> response.address().equals( constrainedServer.toString() ) ).collect( toList() );
        var successful = responses.stream().filter( not( response -> response.address().equals( constrainedServer.toString() ) ) ).collect( toList() );

        assertThat( failed ).hasSize( 1 )
                .allMatch( response -> response.state() == WaitResponseStates.Failed )
                .allMatch( waitResponse -> !waitResponse.success() );
        assertThat( successful ).hasSize( 3 )
                .allMatch( response -> response.state() == WaitResponseStates.CaughtUp )
                .allMatch( waitResponse -> !waitResponse.success() );
        dropDatabase( "bar", cluster );
    }

    @Test
    void shouldWaitForDatabaseToStopOnAllServers() throws Exception
    {
        try
        {

            stopDatabase( databaseId.name(), cluster );
            var lastCommitTxId =
                    cluster.awaitLeader( "system" ).resolveDependency( "system", TransactionIdStore.class ).getLastCommittedTransactionId();

            var responses = runProcedure( systemDbDriver, lastCommitTxId, databaseId, DEFAULT_PROCEDURE_TIMEOUT );
            assertThat( databaseStates( cluster, databaseId.name() ) ).allMatch( operatorState -> operatorState == EnterpriseOperatorState.STOPPED );

            assertThat( responses ).hasSize( cluster.coreMembers().size() + cluster.readReplicas().size() )
                    .allMatch( response -> response.state() == WaitResponseStates.CaughtUp )
                    .allMatch( WaitResponse::success );
        }
        finally
        {
            startDatabase( databaseId.name(), cluster );
            assertDatabaseEventuallyStarted( databaseId.name(), cluster );
        }
    }

    private static List<WaitResponse> runProcedure( Driver driver, long systemTxId, NamedDatabaseId databaseId, Duration timeout )
    {
        try ( Session session = driver.session( SessionConfig.forDatabase( "system" ) ) )
        {
            return session.writeTransaction( tx ->
                    tx.run( "call dbms.admin.wait($systemTxId,$databaseId,$databaseName,$timeout)",
                            Map.of( "systemTxId", systemTxId, "databaseId", databaseId.databaseId().uuid().toString(), "databaseName", databaseId.name(),
                                    "timeout", timeout.getSeconds() ) )
                            .stream()
                            .map(
                                    WaitResponse::createServerResponse )
                            .collect( toList() ) );
        }
    }
}
