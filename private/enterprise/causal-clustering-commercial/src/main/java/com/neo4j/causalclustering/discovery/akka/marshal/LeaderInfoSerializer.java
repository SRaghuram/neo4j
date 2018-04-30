/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;

public class LeaderInfoSerializer extends BaseAkkaSerializer<LeaderInfo>
{
    private static final int SIZE_HINT = 25;

    public LeaderInfoSerializer()
    {
        super( new LeaderInfoMarshal(), BaseAkkaSerializer.LEADER_INFO, SIZE_HINT );
    }
}
