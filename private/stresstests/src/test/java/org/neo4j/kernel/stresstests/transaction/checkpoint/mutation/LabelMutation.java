/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.stresstests.transaction.checkpoint.mutation;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

class LabelMutation implements Mutation
{
    private final GraphDatabaseService db;

    LabelMutation( GraphDatabaseService db )
    {
        this.db = db;
    }

    @Override
    public void perform( long nodeId, String value )
    {
        Node node = db.getNodeById( nodeId );
        Label label = Label.label( value );
        if ( node.hasLabel( label ) )
        {
            node.removeLabel( label );
        }
        else
        {
            node.addLabel( label );
        }
    }
}
