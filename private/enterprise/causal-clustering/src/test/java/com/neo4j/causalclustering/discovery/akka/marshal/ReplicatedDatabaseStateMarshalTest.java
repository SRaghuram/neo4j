/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.dbms.EnterpriseOperatorState;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.kernel.database.DatabaseId;

import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

public class ReplicatedDatabaseStateMarshalTest extends BaseMarshalTest<ReplicatedDatabaseState>
{
    @Override
    public Collection<ReplicatedDatabaseState> originals()
    {
        var dbId = randomDatabaseId();
        var coreStates = ReplicatedDatabaseState.ofCores( dbId, memberStates( dbId, 3 ) );
        var rrStates = ReplicatedDatabaseState.ofReadReplicas( dbId, memberStates( dbId, 2 ) );
        return Set.of( coreStates, rrStates );
    }

    @Override
    public ChannelMarshal<ReplicatedDatabaseState> marshal()
    {
        return ReplicatedDatabaseStateMarshal.INSTANCE;
    }

    private static Map<ServerId,DiscoveryDatabaseState> memberStates( DatabaseId databaseId, int numMembers )
    {
        return Stream.generate( () -> Map.entry( IdFactory.randomServerId(),
                        new DiscoveryDatabaseState( databaseId, EnterpriseOperatorState.STARTED ) ) )
                .limit( numMembers )
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
    }
}
