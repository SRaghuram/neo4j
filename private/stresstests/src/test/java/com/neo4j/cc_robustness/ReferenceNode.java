/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

import static org.neo4j.graphdb.Label.label;

public class ReferenceNode
{
    public static final Label REFERENCE_NODE_LABEL = label( "the node formerly known as the reference node" );

    static void createReferenceNode( GraphDatabaseService db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            ResourceIterator<Node> theReferenceNode = getTheReferenceNodeByLabel( transaction );

            if ( theReferenceNode.hasNext() )
            {
                return;
            }

            transaction.createNode( REFERENCE_NODE_LABEL );

            transaction.commit();
        }
    }

    public static Node findReferenceNode( Transaction transaction )
    {
        ResourceIterator<Node> iterator = getTheReferenceNodeByLabel( transaction );

        if ( !iterator.hasNext() )
        {
            throw new IllegalStateException( "No 'reference node' found." );
        }

        Node referenceNode = iterator.next();

        if ( iterator.hasNext() )
        {
            throw new IllegalStateException( "Too many 'reference nodes' found." );
        }
        return referenceNode;
    }

    private static ResourceIterator<Node> getTheReferenceNodeByLabel( Transaction transaction )
    {
        return transaction.findNodes( REFERENCE_NODE_LABEL );
    }
}
