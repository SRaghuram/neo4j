/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.test.causalclustering.ClusterRule;

public class CausalClusteringRolesIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 1 );

    @Rule
    public ExpectedException exceptionMatcher = ExpectedException.none();

    @Test
    public void readReplicasShouldRefuseWrites() throws Exception
    {
        // given
        Cluster<?> cluster = clusterRule.startCluster();
        GraphDatabaseService db = cluster.findAnyReadReplica().database();
        Transaction tx = db.beginTx();

        // then
        exceptionMatcher.expect( WriteOperationsNotAllowedException.class );

        // when
        db.createNode();
        tx.success();
        tx.close();
    }
}
