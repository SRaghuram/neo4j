/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SkipThreadLeakageGuard;

import static com.neo4j.causalclustering.discovery.InitialDiscoveryMembersResolver.advertisedSocketAddressComparator;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

@SkipThreadLeakageGuard
@ClusterExtension
@TestInstance( PER_METHOD )
public class ClusterFormationIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeEach
    void setup() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 1 ) );
        cluster.start();
    }

    @Test
    void shouldBeAbleToAddAndRemoveCoreMembers() throws Exception
    {
        // when
        CoreClusterMember coreMember = getExistingCoreMember();
        coreMember.shutdown();
        coreMember.start();

        // then
        verifyNumberOfCoresReportedByTopology( 3 );

        // when
        removeCoreMember();

        // then
        verifyNumberOfCoresReportedByTopology( 2 );

        // when
        cluster.newCoreMember().start();

        // then
        verifyNumberOfCoresReportedByTopology( 3 );
    }

    @Test
    void shouldBeAbleToAddAndRemoveCoreMembersUnderModestLoad() throws Exception
    {
        // given
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        GraphDatabaseFacade leader = cluster.awaitLeader().defaultDatabase();
        executorService.submit( () ->
        {
            try ( Transaction tx = leader.beginTx() )
            {
                tx.createNode();
                tx.commit();
            }
        } );

        // when
        CoreClusterMember coreMember = getExistingCoreMember();
        coreMember.shutdown();
        coreMember.start();

        // then
        verifyNumberOfCoresReportedByTopology( 3 );

        // when
        removeCoreMember();

        // then
        verifyNumberOfCoresReportedByTopology( 2 );

        // when
        cluster.newCoreMember().start();

        // then
        verifyNumberOfCoresReportedByTopology( 3 );

        executorService.shutdown();
    }

    @Test
    void shouldBeAbleToRestartTheCluster() throws Exception
    {
        // when started then
        verifyNumberOfCoresReportedByTopology( 3 );

        // when
        cluster.shutdown();
        cluster.start();

        // then
        verifyNumberOfCoresReportedByTopology( 3 );

        // when
        removeCoreMember();

        cluster.newCoreMember().start();
        cluster.shutdown();

        cluster.start();

        // then
        verifyNumberOfCoresReportedByTopology( 3 );
    }

    private CoreClusterMember getExistingCoreMember()
    {
        // return the core listed last in discovery members
        // never return the first core in discovery members because it might be stopped and then Akka cluster can't bootstrap
        return cluster.coreMembers()
                .stream()
                .max( ClusterFormationIT::compareDiscoveryAddresses )
                .orElseThrow();
    }

    private void removeCoreMember()
    {
        var existingCoreMember = getExistingCoreMember();
        cluster.removeCoreMember( existingCoreMember );
    }

    private void verifyNumberOfCoresReportedByTopology( int expected ) throws InterruptedException
    {
        assertEventually( () -> cluster.numberOfCoreMembersReportedByTopology( DEFAULT_DATABASE_NAME ), is( expected ), 30, SECONDS );
    }

    private static int compareDiscoveryAddresses( CoreClusterMember core1, CoreClusterMember core2 )
    {
        return advertisedSocketAddressComparator().compare( discoveryAddress( core1 ), discoveryAddress( core2 ) );
    }

    private static SocketAddress discoveryAddress( CoreClusterMember core )
    {
        return core.config().get( CausalClusteringSettings.discovery_advertised_address );
    }
}
