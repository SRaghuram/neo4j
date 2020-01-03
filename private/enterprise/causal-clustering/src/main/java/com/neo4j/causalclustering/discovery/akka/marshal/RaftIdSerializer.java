/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.identity.RaftId;

public class RaftIdSerializer extends BaseAkkaSerializer<RaftId>
{
    static final int SIZE_HINT = 16;

    public RaftIdSerializer()
    {
        super( new RaftId.Marshal(), RAFT_ID, SIZE_HINT );
    }
}
