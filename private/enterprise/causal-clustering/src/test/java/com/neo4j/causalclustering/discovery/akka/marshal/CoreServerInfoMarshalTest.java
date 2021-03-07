/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.neo4j.io.marshal.ChannelMarshal;

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

public class CoreServerInfoMarshalTest extends BaseMarshalTest<CoreServerInfo>
{
    @Override
    public Collection<CoreServerInfo> originals()
    {
        return List.of(
                addressesForCore( 42, Set.of() ),
                addressesForCore( 4242, Set.of() ),
                addressesForCore( 513, Set.of( randomDatabaseId() ) ),
                addressesForCore( 98738, Set.of( randomDatabaseId() ) ),
                addressesForCore( 145, Set.of( randomDatabaseId(), randomDatabaseId(), randomDatabaseId() ) ),
                addressesForCore( 8361, Set.of( randomDatabaseId(), randomDatabaseId(), randomDatabaseId() ) )
        );
    }

    @Override
    public ChannelMarshal<CoreServerInfo> marshal()
    {
        return new CoreServerInfoMarshal();
    }
}
