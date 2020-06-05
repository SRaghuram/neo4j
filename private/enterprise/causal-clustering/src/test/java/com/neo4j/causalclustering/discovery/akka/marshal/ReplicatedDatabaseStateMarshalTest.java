/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import com.neo4j.dbms.EnterpriseOperatorState;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.kernel.database.DatabaseId;

import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

public class ReplicatedDatabaseStateMarshalTest extends BaseMarshalTest<ReplicatedDatabaseState>
{
    @Override
    Collection<ReplicatedDatabaseState> originals()
    {
        var dbId = randomDatabaseId();
        var coreStates = ReplicatedDatabaseState.ofCores( dbId, memberStates( dbId, 3 ) );
        var rrStates = ReplicatedDatabaseState.ofReadReplicas( dbId, memberStates( dbId, 2 ) );
        return Set.of( coreStates, rrStates );
    }

    @Override
    ChannelMarshal<ReplicatedDatabaseState> marshal()
    {
        return ReplicatedDatabaseStateMarshal.INSTANCE;
    }

    private static Map<MemberId,DiscoveryDatabaseState> memberStates( DatabaseId databaseId, int numMembers )
    {
        return Stream.generate( () -> Map.entry( new MemberId( UUID.randomUUID() ),
                        new DiscoveryDatabaseState( databaseId, EnterpriseOperatorState.STARTED ) ) )
                .limit( numMembers )
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
    }
}
