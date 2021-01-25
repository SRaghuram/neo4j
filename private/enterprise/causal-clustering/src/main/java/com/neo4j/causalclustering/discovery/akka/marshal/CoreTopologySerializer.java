/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;

public class CoreTopologySerializer extends BaseAkkaSerializer<DatabaseCoreTopology>
{
    public static final int SIZE_HINT = 1536;

    public CoreTopologySerializer()
    {
        super( new CoreTopologyMarshal(), CORE_TOPOLOGY, SIZE_HINT );
    }
}
