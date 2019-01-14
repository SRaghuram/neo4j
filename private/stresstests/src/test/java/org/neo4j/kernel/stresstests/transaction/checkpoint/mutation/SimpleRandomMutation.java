/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.stresstests.transaction.checkpoint.mutation;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.GraphDatabaseService;

class SimpleRandomMutation implements RandomMutation
{
    private final long nodeCount;
    private final GraphDatabaseService db;
    private final Mutation rareMutation;
    private final Mutation commonMutation;

    SimpleRandomMutation( long nodeCount, GraphDatabaseService db, Mutation rareMutation, Mutation commonMutation )
    {
        this.nodeCount = nodeCount;
        this.db = db;
        this.rareMutation = rareMutation;
        this.commonMutation = commonMutation;
    }

    private static String[] NAMES =
            {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s"};

    @Override
    public void perform()
    {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        long nodeId = rng.nextLong( nodeCount );
        String value = NAMES[rng.nextInt( NAMES.length )];

        if ( rng.nextDouble() < 0.01 )
        {
            rareMutation.perform( nodeId, value );
        }
        else
        {
            commonMutation.perform( nodeId, value );
        }
    }
}
