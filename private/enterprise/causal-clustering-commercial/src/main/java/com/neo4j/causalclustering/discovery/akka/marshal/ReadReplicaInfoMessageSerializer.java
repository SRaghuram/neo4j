/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.actor.ExtendedActorSystem;

import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaInfoMessage;

public class ReadReplicaInfoMessageSerializer extends BaseAkkaSerializer<ReadReplicaInfoMessage>
{
    protected ReadReplicaInfoMessageSerializer( ExtendedActorSystem system )
    {
        super( new ReadReplicaInfoMessageMarshal( system ), BaseAkkaSerializer.READ_REPLICA_INFO_FOR_MEMBER_ID, 256 );
    }
}
