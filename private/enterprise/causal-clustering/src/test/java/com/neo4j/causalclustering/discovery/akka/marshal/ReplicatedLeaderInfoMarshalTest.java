/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.akka.directory.ReplicatedLeaderInfo;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;
import com.neo4j.causalclustering.identity.IdFactory;

import java.util.Collection;

import org.neo4j.io.marshal.ChannelMarshal;

import static java.util.Collections.singletonList;

public class ReplicatedLeaderInfoMarshalTest extends BaseMarshalTest<ReplicatedLeaderInfo>
{
    @Override
    public Collection<ReplicatedLeaderInfo> originals()
    {
        return singletonList(new ReplicatedLeaderInfo( new LeaderInfo( IdFactory.randomRaftMemberId(), 12L ) ) );
    }

    @Override
    public ChannelMarshal<ReplicatedLeaderInfo> marshal()
    {
        return new ReplicatedLeaderInfoMarshal();
    }
}
