/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.DataCreator;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.WriteOperationsNotAllowedException;
import org.neo4j.io.pagecache.monitoring.PageCacheCounters;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.DataMatching.dataMatchesEventually;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.function.Predicates.await;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.assertion.Assert.assertEventually;

@TestInstance( TestInstance.Lifecycle.PER_METHOD )
@ClusterExtension
class CoreReplicationIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    private final ClusterConfig clusterConfig = ClusterConfig
            .clusterConfig()
            .withNumberOfCoreMembers( 3 )
            .withSharedCoreParam( CausalClusteringSettings.minimum_core_cluster_size_at_formation, "3" )
            .withNumberOfReadReplicas( 0 );

    @BeforeEach
    void setup() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void shouldReplicateTransactionsToCoreMembers() throws Exception
    {
        // when
        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            Node node = tx.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.commit();
        } );

        // then
        assertEquals( 1, DataCreator.countNodes( leader ) );
        dataMatchesEventually( leader, cluster.allMembers() );
    }

    @Test
    void shouldNotAllowWritesFromAFollower() throws TimeoutException
    {
        // given
        cluster.awaitLeader();

        GraphDatabaseFacade follower = cluster.awaitCoreMemberWithRole( Role.FOLLOWER ).defaultDatabase();

        // when
        try ( Transaction tx = follower.beginTx() )
        {
            WriteOperationsNotAllowedException ex = assertThrows( WriteOperationsNotAllowedException.class, tx::createNode );
            assertThat( ex.getMessage(), containsString( "No write operations are allowed" ) );
        }
    }

    @Test
    void pageFaultsFromReplicationMustCountInMetrics() throws Exception
    {
        // Given initial pin counts on all members
        Function<CoreClusterMember,PageCacheCounters> getPageCacheCounters =
                ccm -> ccm.defaultDatabase().getDependencyResolver().resolveDependency( PageCacheCounters.class );
        List<PageCacheCounters> countersList = cluster.coreMembers().stream().map( getPageCacheCounters ).collect( Collectors.toList() );
        long[] initialPins = countersList.stream().mapToLong( PageCacheCounters::pins ).toArray();

        // when the leader commits a write transaction,
        cluster.coreTx( ( db, tx ) ->
        {
            Node node = tx.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.commit();
        } );

        // then the replication should cause pins on a majority of core members to increase.
        // However, the commit returns as soon as the transaction has been replicated through the Raft log, which
        // happens before the transaction is applied on the members. Therefor we are racing with the followers
        // transaction application, so we have to spin.
        int minimumUpdatedMembersCount = countersList.size() / 2 + 1;
        assertEventually( "Expected followers to eventually increase pin counts", () ->
        {
            long[] pinsAfterCommit = countersList.stream().mapToLong( PageCacheCounters::pins ).toArray();
            int membersWithIncreasedPinCount = 0;
            for ( int i = 0; i < initialPins.length; i++ )
            {
                long before = initialPins[i];
                long after = pinsAfterCommit[i];
                if ( before < after )
                {
                    membersWithIncreasedPinCount++;
                }
            }
            return membersWithIncreasedPinCount;
        }, v -> v >= minimumUpdatedMembersCount, 10, SECONDS );
    }

    @Test
    void shouldNotAllowSchemaChangesFromAFollower() throws Exception
    {
        // given
        cluster.awaitLeader();

        GraphDatabaseFacade follower = cluster.awaitCoreMemberWithRole( Role.FOLLOWER ).defaultDatabase();

        // when
        try ( Transaction tx = follower.beginTx() )
        {
            WriteOperationsNotAllowedException ex = assertThrows( WriteOperationsNotAllowedException.class,
                    () -> tx.schema().constraintFor( Label.label( "Foo" ) ).assertPropertyIsUnique( "name" ).create() );
            assertThat( ex.getMessage(), containsString( "No write operations are allowed" ) );
        }
    }

    @Test
    void shouldNotAllowTokenCreationFromAFollowerWithNoInitialTokens() throws Exception
    {
        // given
        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            tx.createNode();
            tx.commit();
        } );

        awaitForDataToBeApplied( leader );
        dataMatchesEventually( leader, cluster.coreMembers() );

        GraphDatabaseFacade follower = cluster.awaitCoreMemberWithRole( Role.FOLLOWER ).defaultDatabase();

        // when
        try ( Transaction tx = follower.beginTx();
              ResourceIterator<Node> allNodes = tx.getAllNodes().iterator()
        )
        {
            WriteOperationsNotAllowedException ex =
                    assertThrows( WriteOperationsNotAllowedException.class, () -> allNodes.next().setProperty( "name", "Mark" ) );
            assertThat( ex.getMessage(), containsString( "No write operations are allowed" ) );
        }
    }

    private void awaitForDataToBeApplied( CoreClusterMember leader ) throws TimeoutException
    {
        await( () -> DataCreator.countNodes(leader) > 0, 10, SECONDS);
    }

    @Test
    void shouldReplicateTransactionToCoreMemberAddedAfterInitialStartUp() throws Exception
    {
        // given
        cluster.getCoreMemberByIndex( 0 ).shutdown();

        cluster.newCoreMember().start();
        cluster.getCoreMemberByIndex( 0 ).start();

        cluster.coreTx( ( db, tx ) ->
        {
            Node node = tx.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.commit();
        } );

        // when
        cluster.newCoreMember().start();
        CoreClusterMember last = cluster.coreTx( ( db, tx ) ->
        {
            Node node = tx.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.commit();
        } );

        // then
        assertEquals( 2, DataCreator.countNodes( last ) );
        dataMatchesEventually( last, cluster.coreMembers() );
    }

    @Test
    void shouldReplicateTransactionAfterLeaderWasRemovedFromCluster() throws Exception
    {
        // given
        cluster.coreTx( ( db, tx ) ->
        {
            Node node = tx.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.commit();
        } );

        // when
        cluster.removeCoreMember( cluster.awaitLeader() );
        cluster.awaitLeader();

        CoreClusterMember last = cluster.coreTx( ( db, tx ) ->
        {
            Node node = tx.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.commit();
        } );

        // then
        assertEquals( 2, DataCreator.countNodes( last ) );
        dataMatchesEventually( last, cluster.coreMembers() );
    }

    @Test
    void shouldReplicateToCoreMembersAddedAfterInitialTransactions() throws Exception
    {
        // when
        CoreClusterMember last = null;
        for ( int i = 0; i < 15; i++ )
        {
            last = cluster.coreTx( ( db, tx ) ->
            {
                Node node = tx.createNode();
                node.setProperty( "foobar", "baz_bat" );
                tx.commit();
            } );
        }

        cluster.newCoreMember().start();
        cluster.newCoreMember().start();

        // then
        assertEquals( 15, DataCreator.countNodes( last ) );
        dataMatchesEventually( last, cluster.coreMembers() );
    }

    @Test
    void shouldReplicateTransactionsToReplacementCoreMembers() throws Exception
    {
        // when
        cluster.coreTx( ( db, tx ) ->
        {
            Node node = tx.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.commit();
        } );

        cluster.removeCoreMemberWithIndex( 0 );
        CoreClusterMember replacement = cluster.addCoreMemberWithIndex( 0 );
        replacement.start();

        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            tx.schema().indexFor( label( "boo" ) ).on( "foobar" ).create();
            tx.commit();
        } );

        // then
        assertEquals( 1, DataCreator.countNodes( leader ) );
        dataMatchesEventually( leader, cluster.coreMembers() );
    }

    @Test
    void shouldBeAbleToShutdownWhenTheLeaderIsTryingToReplicateTransaction() throws Exception
    {
        // given
        cluster.coreTx( ( db, tx ) ->
        {
            Node node = tx.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.commit();
        } );

        CountDownLatch latch = new CountDownLatch( 1 );

        // when
        Thread thread = new Thread( () ->
        {
            try
            {
                cluster.coreTx( ( db, tx ) ->
                {
                    tx.createNode();
                    tx.commit();

                    cluster.removeCoreMember( cluster.getMemberWithAnyRole( Role.FOLLOWER, Role.CANDIDATE ) );
                    cluster.removeCoreMember( cluster.getMemberWithAnyRole( Role.FOLLOWER, Role.CANDIDATE ) );
                    latch.countDown();
                } );
                fail( "Should have thrown" );
            }
            catch ( Exception ignored )
            {
                // expected
            }
        } );

        thread.start();

        latch.await();

        // then the cluster can shutdown...
        cluster.shutdown();
        // ... and the thread running the tx does not get stuck
        thread.join( TimeUnit.MINUTES.toMillis( 1 ) );
    }
}
