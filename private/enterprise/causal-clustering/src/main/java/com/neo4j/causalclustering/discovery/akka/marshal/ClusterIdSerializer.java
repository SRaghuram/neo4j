/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.identity.ClusterId;

public class ClusterIdSerializer extends BaseAkkaSerializer<ClusterId>
{
    static final int SIZE_HINT = 16;

    public ClusterIdSerializer()
    {
        super( new ClusterId.Marshal(), CLUSTER_ID, SIZE_HINT );
    }
}
