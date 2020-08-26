/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage;
import com.neo4j.causalclustering.identity.IdFactory;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.io.marshal.ChannelMarshal;

import static java.util.Collections.singletonList;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

public class LeaderInfoDirectoryMessageMarshalTest extends BaseMarshalTest<LeaderInfoDirectoryMessage>
{
    @Override
    Collection<LeaderInfoDirectoryMessage> originals()
    {
        return singletonList( generate() );
    }

    @Override
    ChannelMarshal<LeaderInfoDirectoryMessage> marshal()
    {
        return new DatabaseLeaderInfoMessageMarshal();
    }

    static LeaderInfoDirectoryMessage generate()
    {
        var leaders = IntStream.range( 0, 5 )
                .mapToObj( id -> Pair.of( randomDatabaseId(), new LeaderInfo( IdFactory.randomRaftMemberId(), id ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );
        return new LeaderInfoDirectoryMessage( leaders );
    }
}
