/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.actor.ExtendedActorSystem;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaRemovalMessage;

public class ReadReplicaRemovalMessageSerializer extends BaseAkkaSerializer<ReadReplicaRemovalMessage>
{
    protected ReadReplicaRemovalMessageSerializer( ExtendedActorSystem system )
    {
        super( new ReadReplicaRemovalMessageMarshal( system ), READ_REPLICA_REMOVAL, 64 );
    }
}
