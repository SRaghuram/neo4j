/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftGroupId;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.io.marshal.ChannelMarshal;

import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

class CoreTopologyMarshalTest extends BaseMarshalTest<DatabaseCoreTopology>
{
    @Override
    public Collection<DatabaseCoreTopology> originals()
    {
        var dbId1 = randomDatabaseId();
        var dbId2 = randomDatabaseId();
        var dbId3 = randomDatabaseId();

        return Arrays.asList(
                new DatabaseCoreTopology( dbId1, RaftGroupId.from( dbId1 ), coreServerInfos( 0 ) ),
                new DatabaseCoreTopology( dbId2, RaftGroupId.from( dbId2 ), coreServerInfos( 3 ) ),
                new DatabaseCoreTopology( dbId3, null, coreServerInfos( 4 ) )
        );
    }

    @Override
    public ChannelMarshal<DatabaseCoreTopology> marshal()
    {
        return new CoreTopologyMarshal();
    }

    static Map<ServerId,CoreServerInfo> coreServerInfos( int count )
    {
        return IntStream.range( 0, count )
                .mapToObj( i -> Pair.of( IdFactory.randomServerId(), TestTopology.addressesForCore( i ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );
    }
}
