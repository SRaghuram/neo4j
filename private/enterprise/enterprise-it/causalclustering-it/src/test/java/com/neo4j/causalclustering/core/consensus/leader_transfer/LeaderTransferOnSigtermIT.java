/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.common.CausalClusteringTestHelpers;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.configuration.CausalClusteringInternalSettings;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.Temporal;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.function.Predicates;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.test.conditions.Conditions;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.showDatabases;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
class LeaderTransferOnSigtermIT
{
    private static final int numberOfShutdownsToTest = 10;
    private static final Duration leaderRemovalTimeout = Duration.ofSeconds( 20 );
    private static final String defaultDatabaseName = DEFAULT_DATABASE_NAME;
    private static final Set<String> databases = Set.of( SYSTEM_DATABASE_NAME, defaultDatabaseName );

    @Inject
    private ClusterFactory clusterFactory;
    private Cluster cluster;
    private ExecutorService executor;

    @BeforeAll
    void setUp() throws Exception
    {
        executor = Executors.newCachedThreadPool();
        cluster = clusterFactory.createCluster(
                clusterConfig()
                        .withNumberOfReadReplicas( 0 )
                        .withNumberOfCoreMembers( 3 )
                        .withSharedCoreParam( CausalClusteringSettings.leader_failure_detection_window, "60s-70s" )
                        .withSharedCoreParam( CausalClusteringSettings.election_failure_detection_window, "1s-3s" )
                        .withSharedCoreParam( CausalClusteringSettings.log_shipping_retry_timeout, "500ms" )
                        .withSharedCoreParam( CausalClusteringInternalSettings.akka_failure_detector_acceptable_heartbeat_pause, "5s" )
                        .withSharedCoreParam( CausalClusteringSettings.minimum_core_cluster_size_at_runtime, "2" )
        );
        cluster.start();
    }

    @AfterAll
    void tearDown() throws InterruptedException
    {
        executor.shutdownNow();
        assert executor.awaitTermination( 1, MINUTES );
    }

    @Test
    void shouldTransferLeadershipOfDbsOnShutdown() throws Throwable
    {
        // given
        var failureDetectionMin = getFailureDetectionWindowMin();
        var failureDetectionMax = getFailureDetectionWindowMax();

        // We expect clusters to stabilise following disruption (such as loss of a core) within 2 failure detection windows
        var clusterStabilityTimeout = failureDetectionMax.multipliedBy( 2 );

        // We know that leader transfer worked if it happens significantly before the failure detection window is reached
        var leadershipTransferSuccessCriteria = failureDetectionMin.dividedBy( 2 );

        // this needs to be large enough that we regularly test the possibility of all 3 original cores being shutdown
        assertThat( numberOfShutdownsToTest ).isGreaterThan( 5 );

        // we have to allow some time after leader removal for leadership transfer to happen
        assertThat( leadershipTransferSuccessCriteria ).isGreaterThan( leaderRemovalTimeout );

        // when
        var successes = 0;

        for ( int i = 0; i < numberOfShutdownsToTest; i++ )
        {
            // given a healthy functioning cluster
            var leaderToRemove = assertClusterHealthyAndGetLeader( defaultDatabaseName, clusterStabilityTimeout );
            var leaderToRemoveMemberId = leaderToRemove.raftMemberIdFor( leaderToRemove.databaseId( defaultDatabaseName ) );
            checkNodeCreation();

            // when we ask to remove a core gracefully ...
            var leaderRemovalDeadline = getDeadlineFor( leaderRemovalTimeout );
            var leaderTransferDeadline = getDeadlineFor( leadershipTransferSuccessCriteria );
            var clusterStabilityDeadline = getDeadlineFor( clusterStabilityTimeout );
            var removeLeader = runWithTestExecutor( () -> cluster.removeCoreMember( leaderToRemove ) );

            // then the call to remove the core succeeds in time
            assertThat( removeLeader ).succeedsWithin( getTimeoutFor( leaderRemovalDeadline ) );

            // then all databases should get a new leader before our deadline elapses
            try
            {
                var newLeaders = supplyWithTestExecutor( newLeaderSupplier( leaderToRemove, getTimeoutFor( leaderTransferDeadline ) ) );
                assertThat( newLeaders ).succeedsWithin( getTimeoutFor( leaderTransferDeadline ) );
                assertThat( newLeaders.get().values() ).doesNotContain( leaderToRemove );
                if ( newLeaders.get().size() == databases.size() )
                {
                    successes++;
                }
            }
            catch ( AssertionError | ExecutionException e )
            {
                if ( e instanceof AssertionError || e.getCause() instanceof AssertionError )
                {
                    // ignore it for now
                }
                else
                {
                    throw e;
                }
            }
            finally
            {
                // then we should still get a new leader eventually - whether or not the leader transfer on shutdown succeeded
                newLeaderSupplier( leaderToRemove, getTimeoutFor( clusterStabilityDeadline ) ).get();
            }

            // Check that cluster is still writeable
            checkNodeCreation();

            // Check that removed Leader has been voted out
            awaitMemberNotInRaftGroup( defaultDatabaseName, leaderToRemoveMemberId, Duration.ofSeconds( 10 ) );

            // now replace the core we removed
            var newMember = cluster.newCoreMember();
            Cluster.startMembers( newMember );
        }

        // then
        // leader transfer isn't guaranteed to work every time but we expect it to succeed more than 2/3rds of the time in this setup.
        assertThat( successes ).isGreaterThan( (2 * numberOfShutdownsToTest) / 3 );
    }

    private CoreClusterMember assertClusterHealthyAndGetLeader( String databaseName, Duration timeout ) throws TimeoutException
    {
        var deadline = getDeadlineFor( timeout );
        var numberOfCoreMembers = 3;

        cluster.awaitAllCoresJoinedAllRaftGroups( databases, timeout.toMillis(), MILLISECONDS );

        assertEventually( cluster::healthyCoreMembers,
                          Conditions.sizeCondition( numberOfCoreMembers ),
                          getTimeoutFor( deadline ).toMillis(), MILLISECONDS );

        assertEventually(
                "SHOW DATABASES should return one row per database per cluster member.",
                () -> showDatabases( cluster ),
                Conditions.sizeCondition( databases.size() * numberOfCoreMembers ),
                getTimeoutFor( deadline ).toMillis(),
                MILLISECONDS
        );

        return cluster.awaitLeader( databaseName, getTimeoutFor( deadline ).toMillis(), MILLISECONDS );
    }

    private Duration getFailureDetectionWindowMin()
    {
        return cluster.coreMembers().stream()
                      .map( c -> c.config().get( CausalClusteringSettings.leader_failure_detection_window ).getMin() )
                      .min( Comparator.naturalOrder() )
                      .get();
    }

    private Duration getFailureDetectionWindowMax()
    {
        return cluster.coreMembers().stream()
                      .map( c -> c.config().get( CausalClusteringSettings.leader_failure_detection_window ).getMax() )
                      .max( Comparator.naturalOrder() )
                      .get();
    }

    private void checkNodeCreation()
    {
        final var createNewNode = runWithTestExecutor( this::createNode );
        assertThat( createNewNode ).succeedsWithin( 1, MINUTES );
    }

    private void createNode()
    {
        try
        {
            CausalClusteringTestHelpers.createNode( defaultDatabaseName, cluster );
        }

        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    /**
     * Poll leadership until we observe that every database has a leader who is not the oldLeader.
     *
     * @param oldLeader
     * @param timeout
     * @return A supplier that provides a map of DatabaseName -> Leader.
     */
    private Supplier<Map<String,CoreClusterMember>> newLeaderSupplier( final CoreClusterMember oldLeader, Duration timeout )
    {
        final var deadline = getDeadlineFor( timeout );
        return () ->
        {
            while ( true )
            {
                assertThat( Instant.now() ).isBefore( deadline ).as( "Timed out waiting for new leaders" );
                var leaderPerDb = databases.stream()
                                           .map( dbName -> Pair.of( dbName, awaitLeader( dbName, getTimeoutFor( deadline ) ) ) )
                                           .filter( p -> p.other() != null )
                                           .collect( Collectors.toMap( Pair::first, Pair::other ) );

                if ( leaderPerDb.values().stream().noneMatch( c -> c.serverId().equals( oldLeader.serverId() ) ) )
                {
                    return leaderPerDb;
                }
                sleepUnchecked( 100 );
            }
        };
    }

    private void awaitMemberNotInRaftGroup( String databaseName, RaftMemberId memberId, Duration timeout ) throws TimeoutException
    {
        Predicates.await( () -> memberNotInRaftGroup( databaseName, memberId ),
                          timeout.toMillis(), MILLISECONDS,
                          100, MILLISECONDS );
    }

    private boolean memberNotInRaftGroup( String databaseName, RaftMemberId memberId )
    {
        var raftMembersUnion = cluster.allMembers().stream()
                                      .flatMap( m -> m.resolveDependency( databaseName, RaftMachine.class ).replicationMembers().stream() )
                                      .collect( Collectors.toSet() );
        return !raftMembersUnion.contains( memberId );
    }

    private CoreClusterMember awaitLeader( String databaseName, Duration timeout )
    {
        try
        {
            return cluster.awaitLeader( databaseName, timeout.toNanos(), TimeUnit.NANOSECONDS );
        }
        catch ( TimeoutException e )
        {
            return null;
        }
    }

    private CompletableFuture<Void> runWithTestExecutor( Runnable task )
    {
        return CompletableFuture.runAsync( task, executor );
    }

    private <T> CompletableFuture<T> supplyWithTestExecutor( Supplier<T> task )
    {
        return CompletableFuture.supplyAsync( task, executor );
    }

    private static Instant getDeadlineFor( Duration timeout )
    {
        assertThat( timeout ).isPositive();
        return Instant.now().plus( timeout );
    }

    private static Duration getTimeoutFor( Temporal deadline )
    {
        var timeout = Duration.between( Instant.now(), deadline );
        if ( timeout.isNegative() )
        {
            throw new AssertionError( "Cannot set a timeout for a deadline in the past" );
        }
        return timeout;
    }

    private static void sleepUnchecked( long millis )
    {
        try
        {
            Thread.sleep( millis );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }
}
