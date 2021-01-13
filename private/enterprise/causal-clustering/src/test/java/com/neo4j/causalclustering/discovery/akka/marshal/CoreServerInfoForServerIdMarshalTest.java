/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreServerInfoForServerId;
import com.neo4j.causalclustering.identity.IdFactory;

import java.util.Collection;

import org.neo4j.io.marshal.ChannelMarshal;

import static java.util.Collections.singletonList;

class CoreServerInfoForServerIdMarshalTest extends BaseMarshalTest<CoreServerInfoForServerId>
{
    @Override
    Collection<CoreServerInfoForServerId> originals()
    {
        return singletonList( new CoreServerInfoForServerId( IdFactory.randomServerId(), TestTopology.addressesForCore( 1 ) ) );
    }

    @Override
    ChannelMarshal<CoreServerInfoForServerId> marshal()
    {
        return new CoreServerInfoForServerIdMarshal();
    }
}
