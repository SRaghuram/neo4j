/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication.session;

import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.io.marshal.ChannelMarshal;

class GlobalSessionTrackerStateMarshalTest implements BaseMarshalTest<GlobalSessionTrackerState>
{
    @Override
    public Collection<GlobalSessionTrackerState> originals()
    {
        return Stream.generate( this::randomGlobalSessionTrackerState )
                     .limit( 5 )
                     .collect( Collectors.toList() );
    }

    private GlobalSessionTrackerState randomGlobalSessionTrackerState()
    {
        var session = new GlobalSession( UUID.randomUUID(), IdFactory.randomRaftMemberId() );
        var state = new GlobalSessionTrackerState();
        state.update( session, randomOperationId(), ThreadLocalRandom.current().nextLong() );
        return state;
    }

    private LocalOperationId randomOperationId()
    {
        return new LocalOperationId( ThreadLocalRandom.current().nextLong(),
                                     ThreadLocalRandom.current().nextLong() );
    }

    @Override
    public ChannelMarshal<GlobalSessionTrackerState> marshal()
    {
        return GlobalSessionTrackerState.Marshal.INSTANCE;
    }
}
