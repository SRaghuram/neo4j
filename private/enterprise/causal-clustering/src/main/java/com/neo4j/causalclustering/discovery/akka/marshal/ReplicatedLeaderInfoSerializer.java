/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.akka.directory.ReplicatedLeaderInfo;

public class ReplicatedLeaderInfoSerializer extends BaseAkkaSerializer<ReplicatedLeaderInfo>
{
    private static final int SIZE_HINT = 25;

    public ReplicatedLeaderInfoSerializer()
    {
        super( new ReplicatedLeaderInfoMarshal(), REPLICATED_LEADER_INFO, SIZE_HINT );
    }
}
