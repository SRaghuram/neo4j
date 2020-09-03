/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.ReplicatedRaftMapping;

public class ReplicatedRaftMappingSerializer extends BaseAkkaSerializer<ReplicatedRaftMapping>
{
    private static final int SIZE_HINT = 1024;

    public ReplicatedRaftMappingSerializer()
    {
        super( ReplicatedRaftMappingMarshal.INSTANCE, REPLICATED_RAFT_MAPPING, SIZE_HINT );
    }
}
