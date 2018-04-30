/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import org.neo4j.causalclustering.discovery.ReadReplicaInfo;

public class ReadReplicaInfoSerializer extends BaseAkkaSerializer<ReadReplicaInfo>
{
    protected ReadReplicaInfoSerializer()
    {
        super( new ReadReplicaInfoMarshal(), BaseAkkaSerializer.READ_REPLICA_INFO, 256 );
    }
}
