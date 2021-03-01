/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.core.consensus.membership.MembershipEntryMarshalTest;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.io.marshal.ChannelMarshal;

public class RaftCoreStateMarshalTest implements BaseMarshalTest<RaftCoreState>
{

    @Override
    public Collection<RaftCoreState> originals()
    {
        return Stream.generate( MembershipEntryMarshalTest::randomMembershipEntry )
                     .map( RaftCoreState::new )
                     .limit( 5 )
                     .collect( Collectors.toList() );
    }

    @Override
    public ChannelMarshal<RaftCoreState> marshal()
    {
        return new RaftCoreState.Marshal();
    }
}
