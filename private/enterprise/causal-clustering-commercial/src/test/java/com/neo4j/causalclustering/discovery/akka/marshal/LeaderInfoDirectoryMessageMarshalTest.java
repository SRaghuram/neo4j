/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.collection.CollectorsUtil;
import org.neo4j.helpers.collection.Pair;

public class LeaderInfoDirectoryMessageMarshalTest extends BaseMarshalTest<LeaderInfoDirectoryMessage>
{
    public LeaderInfoDirectoryMessageMarshalTest()
    {
        super( generate(), new DatabaseLeaderInfoMessageMarshal() );
    }

    static LeaderInfoDirectoryMessage generate()
    {
        Map<String,LeaderInfo> leaders = IntStream.range( 0, 5 )
                .mapToObj( id -> Pair.of( "db" + id, new LeaderInfo( new MemberId( UUID.randomUUID() ), id ) ) )
                .collect( CollectorsUtil.pairsToMap() );

        return new LeaderInfoDirectoryMessage( leaders );
    }
}
