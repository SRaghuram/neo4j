/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.causalclustering.common.CausalClusteringTestHelpers;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.test.extension.Inject;

import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@ClusterExtension
public class ClusteredDbmsReconcilerIT
{
    @Inject
    ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeEach
    void setup() throws Exception
    {
        ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void shouldStopAndFailDatabaseOnUnderlyingPanic() throws Exception
    {
        // given
        CausalClusteringTestHelpers.createDatabase("foo", cluster );
        CausalClusteringTestHelpers.assertDatabaseEventuallyStarted( "foo", cluster );
        cluster.awaitLeader( "foo" );

        var follower = cluster.awaitCoreMemberWithRole( Role.FOLLOWER );
        var fooDb = follower.database("foo");

        var panicService = fooDb.getDependencyResolver().resolveDependency( PanicService.class );
        var databaseStateService = fooDb.getDependencyResolver().resolveDependency( DatabaseStateService.class );

        var fooPanicker = panicService.panickerFor( fooDb.databaseId() );
        var err = new Exception( "Panic cause" );

        // when
        fooPanicker.panic( err );

        // then
        assertEventually( "Reconciler should eventually stop",
                () -> databaseStateService.stateOfDatabase( fooDb.databaseId() ), equalityCondition( STOPPED ), 1, MINUTES );
        assertEquals( err, databaseStateService.causeOfFailure( fooDb.databaseId() ).orElse( null ) );
    }
}
