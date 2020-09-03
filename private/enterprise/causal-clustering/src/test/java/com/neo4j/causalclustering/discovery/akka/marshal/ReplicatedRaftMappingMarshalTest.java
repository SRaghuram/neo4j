/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.ReplicatedRaftMapping;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.neo4j.io.marshal.ChannelMarshal;

import static com.neo4j.causalclustering.identity.IdFactory.randomRaftMemberId;
import static com.neo4j.causalclustering.identity.IdFactory.randomServerId;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

public class ReplicatedRaftMappingMarshalTest extends BaseMarshalTest<ReplicatedRaftMapping>
{
    @Override
    Collection<ReplicatedRaftMapping> originals()
    {
        var mapping1 = ReplicatedRaftMapping.of( randomServerId(), Map.of(
                randomDatabaseId(), randomRaftMemberId(), randomDatabaseId(), randomRaftMemberId() ) );
        var mapping2 = ReplicatedRaftMapping.of( randomServerId(), Map.of( randomDatabaseId(), randomRaftMemberId() ) );
        var mapping3 = ReplicatedRaftMapping.of( randomServerId(), Map.of() );
        return Set.of( mapping1, mapping2, mapping3 );
    }

    @Override
    ChannelMarshal<ReplicatedRaftMapping> marshal()
    {
        return ReplicatedRaftMappingMarshal.INSTANCE;
    }
}
