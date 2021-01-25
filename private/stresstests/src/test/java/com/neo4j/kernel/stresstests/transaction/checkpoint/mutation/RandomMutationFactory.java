/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.stresstests.transaction.checkpoint.mutation;


import org.neo4j.graphdb.GraphDatabaseService;

public class RandomMutationFactory
{
    private RandomMutationFactory()
    {
    }

    public static RandomMutation defaultRandomMutation( long nodeCount, GraphDatabaseService db )
    {
        return new SimpleRandomMutation( nodeCount, db, new LabelMutation( db ), new PropertyMutation( db ) );
    }
}
