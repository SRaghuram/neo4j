/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseId;

import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

public class LeaderInfoDirectoryMessageMarshalTest extends BaseMarshalTest<LeaderInfoDirectoryMessage>
{
    public LeaderInfoDirectoryMessageMarshalTest()
    {
        super( generate(), new DatabaseLeaderInfoMessageMarshal() );
    }

    static LeaderInfoDirectoryMessage generate()
    {
        Map<DatabaseId,LeaderInfo> leaders = IntStream.range( 0, 5 )
                .mapToObj( id -> Pair.of( randomDatabaseId(), new LeaderInfo( new MemberId( UUID.randomUUID() ), id ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );

        return new LeaderInfoDirectoryMessage( leaders );
    }
}
