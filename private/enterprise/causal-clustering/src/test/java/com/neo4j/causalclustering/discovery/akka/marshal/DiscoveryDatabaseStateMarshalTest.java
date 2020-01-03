/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.dbms.EnterpriseOperatorState;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

@RunWith( Parameterized.class )
public class DiscoveryDatabaseStateMarshalTest extends BaseMarshalTest<DiscoveryDatabaseState>
{
    public DiscoveryDatabaseStateMarshalTest( DiscoveryDatabaseState original )
    {
        super( original, DiscoveryDatabaseStateMarshal.INSTANCE );
    }

    @Parameterized.Parameters
    public static Collection<DiscoveryDatabaseState> parameters()
    {
        return Arrays.stream( EnterpriseOperatorState.values() )
                .map( state -> new DiscoveryDatabaseState( randomDatabaseId(), state ) )
                .collect( Collectors.toList() );
    }
}
