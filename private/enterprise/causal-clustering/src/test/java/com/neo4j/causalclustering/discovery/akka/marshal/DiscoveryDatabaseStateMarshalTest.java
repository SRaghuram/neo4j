/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;
import com.neo4j.dbms.EnterpriseOperatorState;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.neo4j.io.marshal.ChannelMarshal;

import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

public class DiscoveryDatabaseStateMarshalTest implements BaseMarshalTest<DiscoveryDatabaseState>
{
    @Override
    public Collection<DiscoveryDatabaseState> originals()
    {
        return Arrays.stream( EnterpriseOperatorState.values() )
                .map( state -> new DiscoveryDatabaseState( randomDatabaseId(), state ) )
                .collect( Collectors.toList() );
    }

    @Override
    public ChannelMarshal<DiscoveryDatabaseState> marshal()
    {
        return DiscoveryDatabaseStateMarshal.INSTANCE;
    }
}
