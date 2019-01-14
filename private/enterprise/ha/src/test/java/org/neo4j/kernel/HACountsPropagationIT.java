/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

public class HACountsPropagationIT
{
    private static final int PULL_INTERVAL = 100;

    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withSharedSetting( HaSettings.pull_interval, PULL_INTERVAL + "ms" );

    @Test
    public void shouldPropagateNodeCountsInHA() throws TransactionFailureException
    {
        ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        try ( Transaction tx = master.beginTx() )
        {
            master.createNode();
            master.createNode( Label.label( "A" ) );
            tx.success();
        }

        cluster.sync();

        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            try ( org.neo4j.internal.kernel.api.Transaction tx = db.getDependencyResolver().resolveDependency( Kernel.class )
                    .beginTransaction( explicit, AUTH_DISABLED ) )
            {
                assertEquals( 2, tx.dataRead().countsForNode( -1 ) );
                assertEquals( 1, tx.dataRead().countsForNode( 0 /* A */ ) );
            }
        }
    }

    @Test
    public void shouldPropagateRelationshipCountsInHA() throws TransactionFailureException
    {
        ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        try ( Transaction tx = master.beginTx() )
        {
            Node left = master.createNode();
            Node right = master.createNode( Label.label( "A" ) );
            left.createRelationshipTo( right, RelationshipType.withName( "Type" ) );
            tx.success();
        }

        cluster.sync();

        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            try ( org.neo4j.internal.kernel.api.Transaction tx = db.getDependencyResolver().resolveDependency( Kernel.class )
                    .beginTransaction( explicit, AUTH_DISABLED ) )
            {
                assertEquals( 1, tx.dataRead().countsForRelationship( -1, -1, -1 ) );
                assertEquals( 1, tx.dataRead().countsForRelationship( -1, -1, 0 ) );
                assertEquals( 1, tx.dataRead().countsForRelationship( -1, 0, -1 ) );
                assertEquals( 1, tx.dataRead().countsForRelationship( -1, 0, 0 ) );
            }
        }
    }
}
