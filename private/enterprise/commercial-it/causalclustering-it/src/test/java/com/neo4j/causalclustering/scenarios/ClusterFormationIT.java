/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.kernel.enterprise.api.security.CommercialLoginContext;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.extension.Inject;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
public class ClusterFormationIT
{
    @Inject
    private ClusterFactory clusterFactory;

    public final ClusterConfig clusterConfig = ClusterConfig.clusterConfig().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 1 );

    private Cluster cluster;

    @BeforeAll
    void setup() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void shouldSupportBuiltInProcedures()
    {
        Stream.concat(
            cluster.readReplicas().stream().map( ReadReplica::defaultDatabase ),
            cluster.coreMembers().stream().map(CoreClusterMember::defaultDatabase )
        ).forEach( gdb ->
        {
            // (1) BuiltInProcedures from community
            try ( var transaction = gdb.beginTx() )
            {
                try ( var result = gdb.execute( "CALL dbms.procedures()" ) )
                {
                    assertTrue( result.hasNext() );
                }
            }

            // (2) BuiltInProcedures from enterprise
            try ( InternalTransaction tx = gdb.beginTransaction( KernelTransaction.Type.explicit, CommercialLoginContext.AUTH_DISABLED ) )
            {
                try ( Result result = gdb.execute( "CALL dbms.listQueries()" ) )
                {
                    assertTrue( result.hasNext() );
                }

                tx.commit();
            }
        } );
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
        return Iterables.last( cluster.coreMembers() );
    }

    private void removeCoreMember()
    {
        cluster.removeCoreMember( getExistingCoreMember() );
    }

    private void verifyNumberOfCoresReportedByTopology( int expected ) throws InterruptedException
    {
        assertEventually( () -> cluster.numberOfCoreMembersReportedByTopology( DEFAULT_DATABASE_NAME ), is( expected ), 30, SECONDS );
    }
}
