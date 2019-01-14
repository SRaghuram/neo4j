/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.akka.coretopology.CoreServerInfoForMemberId;

public class CoreServerInfoForMemberIdSerializer extends BaseAkkaSerializer<CoreServerInfoForMemberId>
{
    private static final int SIZE_HINT = 512;

    public CoreServerInfoForMemberIdSerializer()
    {
        super( new CoreServerInfoForMemberIdMarshal(), BaseAkkaSerializer.CORE_SERVER_INFO_FOR_MEMBER_ID, SIZE_HINT );
    }
}
