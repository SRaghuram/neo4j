/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.actor.ExtendedActorSystem;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaRefreshMessage;

public class ReadReplicaRefreshMessageSerializer extends BaseAkkaSerializer<ReadReplicaRefreshMessage>
{
    protected ReadReplicaRefreshMessageSerializer( ExtendedActorSystem system )
    {
        super( new ReadReplicaRefreshMessageMarshal( system ), BaseAkkaSerializer.READ_REPLICA_INFO_FOR_MEMBER_ID, 256 );
    }
}
