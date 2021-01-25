/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;

public class ReplicatedDatabaseStateSerializer extends BaseAkkaSerializer<ReplicatedDatabaseState>
{
    private static final int SIZE_HINT = 1024;

    public ReplicatedDatabaseStateSerializer()
    {
        super( ReplicatedDatabaseStateMarshal.INSTANCE, REPLICATED_DATABASE_STATE, SIZE_HINT );
    }
}
