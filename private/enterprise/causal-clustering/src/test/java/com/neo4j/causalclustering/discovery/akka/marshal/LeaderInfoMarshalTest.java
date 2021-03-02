/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;

import java.util.Collection;
import java.util.List;

import org.neo4j.io.marshal.ChannelMarshal;

public class LeaderInfoMarshalTest implements BaseMarshalTest<LeaderInfo>
{
    @Override
    public Collection<LeaderInfo> originals()
    {
        return List.of( new LeaderInfo( IdFactory.randomRaftMemberId(), 12L ) );
    }

    @Override
    public ChannelMarshal<LeaderInfo> marshal()
    {
        return LeaderInfoMarshal.INSTANCE;
    }
}
