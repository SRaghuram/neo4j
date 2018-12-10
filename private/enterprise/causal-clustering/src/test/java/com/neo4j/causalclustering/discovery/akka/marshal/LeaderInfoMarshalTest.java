/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.UUID;

public class LeaderInfoMarshalTest extends BaseMarshalTest<LeaderInfo>
{
    public LeaderInfoMarshalTest()
    {
        super( new LeaderInfo( new MemberId( UUID.randomUUID() ), 12L ), new LeaderInfoMarshal() );
    }
}
