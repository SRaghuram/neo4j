/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt.txtracking;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStopped;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.stopDatabase;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.test.conditions.Conditions.TRUE;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@ClusterExtension
@TestInstance( PER_METHOD )
class ClusterReconciledTransactionTrackerIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeEach
    void beforeEach() throws Exception
    {
        var clusterConfig = clusterConfig().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 2 );
        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void shouldInitializeReconciledTransactionIdAfterStart() throws Exception
    {
        var memberToLastClosedSystemTxId = lastClosedSystemTxIdsForClusterMembers();

        for ( var entry : memberToLastClosedSystemTxId.entrySet() )
        {
            var member = entry.getKey();
            var lastClosedSystemTxId = entry.getValue();
            assertEventually( () -> lastReconciledTxId( member ), equalityCondition( lastClosedSystemTxId ), 1, MINUTES );
        }
    }

    @Test
    void shouldUpdateReconciledTransactionId() throws Exception
    {
        var dbName1 = "foo";
        var dbName2 = "bar";
        var dbName3 = "baz";

        var memberToLastClosedSystemTxIdBefore = lastClosedSystemTxIdsForClusterMembers();

        createDatabase( dbName1, cluster );
        assertDatabaseEventuallyStarted( dbName1, cluster );
        createDatabase( dbName2, cluster );
        assertDatabaseEventuallyStarted( dbName2, cluster );
        createDatabase( dbName3, cluster );
        assertDatabaseEventuallyStarted( dbName3, cluster );
        stopDatabase( dbName2, cluster );
        assertDatabaseEventuallyStopped( dbName2, cluster );
        stopDatabase( dbName1, cluster );
        assertDatabaseEventuallyStopped( dbName1, cluster );

        var memberToLastClosedSystemTxIdAfter = lastClosedSystemTxIdsForClusterMembers();

        assertEquals( memberToLastClosedSystemTxIdBefore.keySet(), memberToLastClosedSystemTxIdAfter.keySet() );

        for ( var member : memberToLastClosedSystemTxIdBefore.keySet() )
        {
            var lastClosedSystemTxIdAfter = memberToLastClosedSystemTxIdAfter.get( member );
            var lastClosedSystemTxIdBefore = memberToLastClosedSystemTxIdBefore.get( member );
            assertThat( lastClosedSystemTxIdAfter, greaterThan( lastClosedSystemTxIdBefore ) );
        }

        for ( var entry : memberToLastClosedSystemTxIdAfter.entrySet() )
        {
            var member = entry.getKey();
            var lastClosedSystemTxIdAfter = entry.getValue();
            assertEventually( () -> lastReconciledTxId( member ), equalityCondition( lastClosedSystemTxIdAfter ), 1, MINUTES );
        }
    }

    private Map<ClusterMember,Long> lastClosedSystemTxIdsForClusterMembers()
    {
        return cluster.allMembers().stream().collect( toMap( identity(), this::lastClosedSystemTxId ) );
    }

    private long lastClosedSystemTxId( ClusterMember member )
    {
        var systemDb = member.systemDatabase();
        assertAvailable( systemDb );
        return resolve( systemDb, TransactionIdStore.class ).getLastClosedTransactionId();
    }

    private static long lastReconciledTxId( ClusterMember member )
    {
        var systemDb = member.systemDatabase();
        assertAvailable( systemDb );
        return resolve( systemDb, ReconciledTransactionTracker.class ).getLastReconciledTransactionId();
    }

    private static void assertAvailable( GraphDatabaseAPI db )
    {
        var availabilityGuard = resolve( db, DatabaseAvailabilityGuard.class );
        assertEventually( availabilityGuard::isAvailable, TRUE, 1, MINUTES );
    }

    private static <T> T resolve( GraphDatabaseAPI db, Class<T> clazz )
    {
        return db.getDependencyResolver().resolveDependency( clazz );
    }
}
