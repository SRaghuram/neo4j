/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.akka.database.state.ReplicatedDatabaseState;
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
