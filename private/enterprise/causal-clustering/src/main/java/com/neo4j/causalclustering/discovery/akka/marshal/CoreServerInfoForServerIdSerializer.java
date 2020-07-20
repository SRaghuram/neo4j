/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.akka.coretopology.CoreServerInfoForServerId;

public class CoreServerInfoForServerIdSerializer extends BaseAkkaSerializer<CoreServerInfoForServerId>
{
    private static final int SIZE_HINT = 512;

    public CoreServerInfoForServerIdSerializer()
    {
        super( new CoreServerInfoForServerIdMarshal(), CORE_SERVER_INFO_FOR_MEMBER_ID, SIZE_HINT );
    }
}
