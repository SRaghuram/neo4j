/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.stresstests.transaction.checkpoint.mutation;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 * Created by davide on 22/06/15.
 */
class PropertyMutation implements Mutation
{
    private final GraphDatabaseService db;

    PropertyMutation( GraphDatabaseService db )
    {
        this.db = db;
    }

    @Override
    public void perform( Transaction tx, long nodeId, String value )
    {
        Node node = tx.getNodeById( nodeId );
        if ( node.hasProperty( value ) )
        {
            node.removeProperty( value );
        }
        else
        {
            node.setProperty( value, 10 );
        }
    }
}
