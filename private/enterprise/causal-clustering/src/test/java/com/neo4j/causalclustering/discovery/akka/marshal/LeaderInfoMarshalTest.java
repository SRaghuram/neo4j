/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.identity.IdFactory;

import java.util.Collection;

import org.neo4j.io.marshal.ChannelMarshal;

import static java.util.Collections.singletonList;

public class LeaderInfoMarshalTest extends BaseMarshalTest<LeaderInfo>
{
    @Override
    Collection<LeaderInfo> originals()
    {
        return singletonList( new LeaderInfo( IdFactory.randomRaftMemberId(), 12L ) );
    }

    @Override
    ChannelMarshal<LeaderInfo> marshal()
    {
        return new LeaderInfoMarshal();
    }
}
