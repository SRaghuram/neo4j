/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import org.neo4j.causalclustering.discovery.CoreTopology;

public class CoreTopologySerializer extends BaseAkkaSerializer<CoreTopology>
{
    public static final int SIZE_HINT = 1536;

    public CoreTopologySerializer()
    {
        super( new CoreTopologyMarshal(), BaseAkkaSerializer.CORE_TOPOLOGY, SIZE_HINT );
    }
}
