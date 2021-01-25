/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;

public class DiscoveryDatabaseStateSerializer extends BaseAkkaSerializer<DiscoveryDatabaseState>
{
    public static final int SIZE_HINT = 128;

    public DiscoveryDatabaseStateSerializer()
    {
        super( DiscoveryDatabaseStateMarshal.INSTANCE, DATABASE_STATE, SIZE_HINT );
    }
}
