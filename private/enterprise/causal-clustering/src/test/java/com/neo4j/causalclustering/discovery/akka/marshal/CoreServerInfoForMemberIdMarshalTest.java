/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreServerInfoForMemberId;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Collection;
import java.util.UUID;

import org.neo4j.io.marshal.ChannelMarshal;

import static java.util.Collections.singletonList;

class CoreServerInfoForMemberIdMarshalTest extends BaseMarshalTest<CoreServerInfoForMemberId>
{
    @Override
    Collection<CoreServerInfoForMemberId> originals()
    {
        return singletonList( new CoreServerInfoForMemberId( new MemberId( UUID.randomUUID() ), TestTopology.addressesForCore( 1, false ) ) );
    }

    @Override
    ChannelMarshal<CoreServerInfoForMemberId> marshal()
    {
        return new CoreServerInfoForMemberIdMarshal();
    }
}
