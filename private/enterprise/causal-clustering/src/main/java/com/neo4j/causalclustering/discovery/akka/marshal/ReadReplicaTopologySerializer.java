/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;

public class ReadReplicaTopologySerializer extends BaseAkkaSerializer<DatabaseReadReplicaTopology>
{
    protected ReadReplicaTopologySerializer()
    {
        super( new ReadReplicaTopologyMarshal(), READ_REPLICA_TOPOLOGY, 1024 );
    }
}
