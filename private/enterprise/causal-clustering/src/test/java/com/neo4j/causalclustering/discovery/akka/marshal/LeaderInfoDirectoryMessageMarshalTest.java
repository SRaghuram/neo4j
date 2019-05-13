/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import org.neo4j.internal.helpers.collection.CollectorsUtil;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

public class LeaderInfoDirectoryMessageMarshalTest extends BaseMarshalTest<LeaderInfoDirectoryMessage>
{
    public LeaderInfoDirectoryMessageMarshalTest()
    {
        super( generate(), new DatabaseLeaderInfoMessageMarshal() );
    }

    static LeaderInfoDirectoryMessage generate()
    {
        var databaseIdRepository = new TestDatabaseIdRepository();
        Map<DatabaseId,LeaderInfo> leaders = IntStream.range( 0, 5 )
                .mapToObj( id -> Pair.of( databaseIdRepository.get( "db" + id ), new LeaderInfo( new MemberId( UUID.randomUUID() ), id ) ) )
                .collect( CollectorsUtil.pairsToMap() );

        return new LeaderInfoDirectoryMessage( leaders );
    }
}
