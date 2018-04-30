/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import java.util.UUID;

import org.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.causalclustering.discovery.akka.CoreServerInfoForMemberId;
import org.neo4j.causalclustering.identity.MemberId;

public class CoreServerInfoForMemberIdMarshalTest extends BaseMarshalTest<CoreServerInfoForMemberId>
{
    public CoreServerInfoForMemberIdMarshalTest()
    {
        super( new CoreServerInfoForMemberId( new MemberId( UUID.randomUUID() ), TestTopology.addressesForCore( 1, false ) ),
                new CoreServerInfoForMemberIdMarshal() );
    }
}
