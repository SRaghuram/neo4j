/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.readreplica.ReadReplica;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.test.causalclustering.ClusterConfig;
import org.neo4j.test.causalclustering.ClusterExtension;
import org.neo4j.test.causalclustering.ClusterFactory;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

@ClusterExtension
public class ClusterFormationIT
{
    @Inject
    private ClusterFactory clusterFactory;

    public final ClusterConfig clusterConfig = ClusterConfig.clusterConfig().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 1 );

    private Cluster<?> cluster;

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
            cluster.readReplicas().stream().map( ReadReplica::database),
            cluster.coreMembers().stream().map(CoreClusterMember::database)
        ).forEach( gdb ->
        {
            // (1) BuiltInProcedures from community
            {
                Result result = gdb.execute( "CALL dbms.procedures()" );
                assertTrue( result.hasNext() );
                result.close();
            }

            // (2) BuiltInProcedures from enterprise
            try ( InternalTransaction tx = gdb.beginTransaction( KernelTransaction.Type.explicit, EnterpriseLoginContext.AUTH_DISABLED ) )
            {
                Result result = gdb.execute( tx, "CALL dbms.listQueries()", EMPTY_MAP );
                assertTrue( result.hasNext() );
                result.close();

                tx.success();
            }
        } );
    }

    @Test
    void shouldBeAbleToAddAndRemoveCoreMembers()
    {
        // when
        CoreClusterMember coreMember = getExistingCoreMember();
        coreMember.shutdown();
        coreMember.start();

        // then
        assertEquals( 3, cluster.numberOfCoreMembersReportedByTopology() );

        // when
        removeCoreMember();

        // then
        assertEquals( 2, cluster.numberOfCoreMembersReportedByTopology() );

        // when
        cluster.newCoreMember().start();

        // then
        assertEquals( 3, cluster.numberOfCoreMembersReportedByTopology() );
    }

    @Test
    void shouldBeAbleToAddAndRemoveCoreMembersUnderModestLoad()
    {
        // given
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit( () ->
        {
            CoreGraphDatabase leader = cluster.getMemberWithRole( Role.LEADER ).database();
            try ( Transaction tx = leader.beginTx() )
            {
                leader.createNode();
                tx.success();
            }
        } );

        // when
        CoreClusterMember coreMember = getExistingCoreMember();
        coreMember.shutdown();
        coreMember.start();

        // then
        assertEquals( 3, cluster.numberOfCoreMembersReportedByTopology() );

        // when
        removeCoreMember();

        // then
        assertEquals( 2, cluster.numberOfCoreMembersReportedByTopology() );

        // when
        cluster.newCoreMember().start();

        // then
        assertEquals( 3, cluster.numberOfCoreMembersReportedByTopology() );

        executorService.shutdown();
    }

    @Test
    void shouldBeAbleToRestartTheCluster() throws Exception
    {
        // when started then
        assertEquals( 3, cluster.numberOfCoreMembersReportedByTopology() );

        // when
        cluster.shutdown();
        cluster.start();

        // then
        assertEquals( 3, cluster.numberOfCoreMembersReportedByTopology() );

        // when
        removeCoreMember();

        cluster.newCoreMember().start();
        cluster.shutdown();

        cluster.start();

        // then
        assertEquals( 3, cluster.numberOfCoreMembersReportedByTopology() );
    }

    private CoreClusterMember getExistingCoreMember()
    {
        return cluster.coreMembers().stream().findFirst().orElseThrow( () -> new IllegalStateException( "Could not find any available cores" ) );
    }

    private void removeCoreMember()
    {
        cluster.removeCoreMember( getExistingCoreMember() );
    }
}
