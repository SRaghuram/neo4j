/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.dbms.EnterpriseDatabaseState;
import com.neo4j.dbms.EnterpriseOperatorState;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.dbms.DatabaseState;
import org.neo4j.kernel.database.DatabaseId;

import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

@RunWith( Parameterized.class )
public class ReplicatedDatabaseStateMarshalTest extends BaseMarshalTest<ReplicatedDatabaseState>
{
    public ReplicatedDatabaseStateMarshalTest( ReplicatedDatabaseState original )
    {
        super( original, ReplicatedDatabaseStateMarshal.INSTANCE );
    }

    @Parameterized.Parameters
    public static Collection<ReplicatedDatabaseState> parameters()
    {
        var dbId = randomDatabaseId();
        var coreStates = ReplicatedDatabaseState.ofCores( dbId, memberStates( dbId, 3 ) );
        var rrStates = ReplicatedDatabaseState.ofReadReplicas( dbId, memberStates( dbId, 2 ) );
        return Set.of( coreStates, rrStates );
    }

    private static Map<MemberId,DatabaseState> memberStates( DatabaseId databaseId, int numMembers )
    {
        return Stream.generate( () -> Map.entry( new MemberId( UUID.randomUUID() ),
                        new EnterpriseDatabaseState( databaseId, EnterpriseOperatorState.STARTED ) ) )
                .limit( numMembers )
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
    }
}
