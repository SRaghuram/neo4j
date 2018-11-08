/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.helpers;

import java.util.function.Supplier;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Pair;

import static org.neo4j.helpers.collection.Iterables.count;

public class DataCreator
{
    private DataCreator()
    {
    }

    public static CoreClusterMember createLabelledNodesWithProperty( Cluster<?> cluster, int numberOfNodes,
            Label label, Supplier<Pair<String,Object>> propertyPair ) throws Exception
    {
        CoreClusterMember last = null;
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            last = cluster.coreTx( ( db, tx ) ->
            {
                Node node = db.createNode( label );
                node.setProperty( propertyPair.get().first(), propertyPair.get().other() );
                tx.success();
            } );
        }
        return last;
    }

    public static CoreClusterMember createEmptyNodes( Cluster<?> cluster, int numberOfNodes ) throws Exception
    {
        CoreClusterMember last = null;
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            last = cluster.coreTx( ( db, tx ) ->
            {
                db.createNode();
                tx.success();
            } );
        }
        return last;
    }

    public static long countNodes( CoreClusterMember member )
    {
        CoreGraphDatabase db = member.database();
        long count;
        try ( Transaction tx = db.beginTx() )
        {
            count = count( db.getAllNodes() );
            tx.success();
        }
        return count;
    }
}
