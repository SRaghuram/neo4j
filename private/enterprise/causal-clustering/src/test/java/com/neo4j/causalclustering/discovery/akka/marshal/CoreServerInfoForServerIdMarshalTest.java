/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreServerInfoForServerId;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;
import com.neo4j.causalclustering.identity.IdFactory;

import java.util.Collection;
import java.util.List;

import org.neo4j.io.marshal.ChannelMarshal;

class CoreServerInfoForServerIdMarshalTest extends BaseMarshalTest<CoreServerInfoForServerId>
{
    @Override
    public Collection<CoreServerInfoForServerId> originals()
    {
        return List.of( new CoreServerInfoForServerId( IdFactory.randomServerId(), TestTopology.addressesForCore( 1 ) ) );
    }

    @Override
    public ChannelMarshal<CoreServerInfoForServerId> marshal()
    {
        return new CoreServerInfoForServerIdMarshal();
    }
}
