/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;

import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.io.marshal.ChannelMarshal;

public class MembershipEntryMarshalTest implements BaseMarshalTest<MembershipEntry>
{
    @Override
    public Collection<MembershipEntry> originals()
    {
        return Stream.generate( MembershipEntryMarshalTest::randomMembershipEntry )
                     .limit( 5 )
                     .collect( Collectors.toList() );
    }

    public static MembershipEntry randomMembershipEntry()
    {
        var numMembers = ThreadLocalRandom.current().nextInt( 1, 100 );
        var members = Stream.generate( IdFactory::randomRaftMemberId )
                            .limit( numMembers )
                            .collect( Collectors.toSet() );
        var logIndex = ThreadLocalRandom.current().nextLong();
        return new MembershipEntry( logIndex, members );
    }

    @Override
    public ChannelMarshal<MembershipEntry> marshal()
    {
        return new MembershipEntry.Marshal();
    }
}
