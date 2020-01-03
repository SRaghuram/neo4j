/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.internal.helpers.collection.Pair;

import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

@RunWith( Parameterized.class )
public class CoreTopologyMarshalTest extends BaseMarshalTest<DatabaseCoreTopology>
{
    public CoreTopologyMarshalTest( DatabaseCoreTopology original )
    {
        super( original, new CoreTopologyMarshal() );
    }

    @Parameterized.Parameters
    public static Collection<DatabaseCoreTopology> data()
    {

        var dbId1 = randomDatabaseId();
        var dbId2 = randomDatabaseId();
        var dbId3 = randomDatabaseId();

        return Arrays.asList(
                new DatabaseCoreTopology( dbId1, RaftId.from( dbId1 ), coreServerInfos( 0 ) ),
                new DatabaseCoreTopology( dbId2, RaftId.from( dbId2 ), coreServerInfos( 3 ) ),
                new DatabaseCoreTopology( dbId3, null, coreServerInfos( 4 ) )
        );
    }

    static Map<MemberId,CoreServerInfo> coreServerInfos( int count )
    {
        return IntStream.range( 0, count )
                .mapToObj( i -> Pair.of( new MemberId( UUID.randomUUID() ), TestTopology.addressesForCore( i, false ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );
    }
}
