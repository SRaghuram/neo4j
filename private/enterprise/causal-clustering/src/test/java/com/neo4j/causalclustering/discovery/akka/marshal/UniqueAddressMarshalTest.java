/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.actor.Address;
import akka.cluster.UniqueAddress;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;
import java.util.Collection;
import java.util.List;

import org.neo4j.io.marshal.ChannelMarshal;

class UniqueAddressMarshalTest extends BaseMarshalTest<UniqueAddress>
{
    @Override
    public Collection<UniqueAddress> originals()
    {
        return List.of(
                new UniqueAddress( new Address( "protocol", "system" ), 17L ),
                new UniqueAddress( new Address( "protocol", "system", "host", 87 ), 17L )
        );
    }

    @Override
    public ChannelMarshal<UniqueAddress> marshal()
    {
        return new UniqueAddressMarshal();
    }
}
