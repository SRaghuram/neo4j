/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreServerInfoForMemberId;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.UUID;

public class CoreServerInfoForMemberIdMarshalTest extends BaseMarshalTest<CoreServerInfoForMemberId>
{
    public CoreServerInfoForMemberIdMarshalTest()
    {
        super( new CoreServerInfoForMemberId( new MemberId( UUID.randomUUID() ), TestTopology.addressesForCore( 1, false ) ),
                new CoreServerInfoForMemberIdMarshal() );
    }
}
