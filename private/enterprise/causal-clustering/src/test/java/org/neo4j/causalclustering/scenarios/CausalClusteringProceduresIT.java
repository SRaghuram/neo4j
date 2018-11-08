/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Optional;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.causalclustering.readreplica.ReadReplica;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext.AUTH_DISABLED;

public class CausalClusteringProceduresIT
{
    @ClassRule
    public static final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 2 )
            .withNumberOfReadReplicas( 1 );

    private static Cluster<?> cluster;

    @BeforeClass
    public static void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void coreProceduresShouldBeAvailable()
    {
        String[] coreProcs = new String[]{
                "dbms.cluster.role", // Server role
                "dbms.cluster.routing.getServers", // Discover the cluster topology
                "dbms.cluster.overview", // Discover appropriate discovery service
                "dbms.procedures", // Kernel built procedures
//                "dbms.security.listUsers", // Security procedure from community
                "dbms.listQueries" // Built in procedure from enterprise
        };

        for ( String procedure : coreProcs )
        {
            Optional<CoreClusterMember> firstCore = cluster.coreMembers().stream().findFirst();
            assert firstCore.isPresent();
            CoreGraphDatabase database = firstCore.get().database();
            InternalTransaction tx = database.beginTransaction( KernelTransaction.Type.explicit, AUTH_DISABLED );
            Result coreResult = database.execute( "CALL " + procedure + "()" );
            assertTrue( "core with procedure " + procedure, coreResult.hasNext() );
            coreResult.close();
            tx.close();
        }
    }

    @Test
    public void readReplicaProceduresShouldBeAvailable()
    {
        // given
        String[] readReplicaProcs = new String[]{
                "dbms.cluster.role", // Server role
                "dbms.procedures", // Kernel built procedures
//                "dbms.security.listUsers", // Security procedure from community
                "dbms.listQueries" // Built in procedure from enterprise
        };

        // when
        for ( String procedure : readReplicaProcs )
        {
            Optional<ReadReplica> firstReadReplica = cluster.readReplicas().stream().findFirst();
            assert firstReadReplica.isPresent();
            ReadReplicaGraphDatabase database = firstReadReplica.get().database();
            InternalTransaction tx = database.beginTransaction( KernelTransaction.Type.explicit, AUTH_DISABLED );
            Result readReplicaResult = database.execute( "CALL " + procedure + "()" );

            // then
            assertTrue( "read replica with procedure " + procedure, readReplicaResult.hasNext() );
            readReplicaResult.close();
            tx.close();
        }
    }
}

