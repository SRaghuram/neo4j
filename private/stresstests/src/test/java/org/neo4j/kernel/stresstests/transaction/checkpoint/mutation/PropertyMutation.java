/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.stresstests.transaction.checkpoint.mutation;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

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
    public void perform( long nodeId, String value )
    {
        Node node = db.getNodeById( nodeId );
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
