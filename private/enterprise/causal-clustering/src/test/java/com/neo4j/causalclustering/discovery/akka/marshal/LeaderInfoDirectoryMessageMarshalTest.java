/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.io.marshal.ChannelMarshal;

import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

public class LeaderInfoDirectoryMessageMarshalTest implements BaseMarshalTest<LeaderInfoDirectoryMessage>
{
    @Override
    public Collection<LeaderInfoDirectoryMessage> originals()
    {
        return List.of( generate() );
    }

    @Override
    public ChannelMarshal<LeaderInfoDirectoryMessage> marshal()
    {
        return DatabaseLeaderInfoMessageMarshal.INSTANCE;
    }

    static LeaderInfoDirectoryMessage generate()
    {
        var leaders = IntStream.range( 0, 5 )
                .mapToObj( id -> Pair.of( randomDatabaseId(), new LeaderInfo( IdFactory.randomRaftMemberId(), id ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );
        return new LeaderInfoDirectoryMessage( leaders );
    }
}
