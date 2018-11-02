/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import java.util.Optional;

import org.neo4j.causalclustering.identity.MemberId;

public class ClusterTopology
{
    private final CoreTopology coreTopology;
    private final ReadReplicaTopology readReplicaTopology;

    public ClusterTopology( CoreTopology coreTopology, ReadReplicaTopology readReplicaTopology )
    {
        this.coreTopology = coreTopology;
        this.readReplicaTopology = readReplicaTopology;
    }

    public Optional<CatchupServerAddress> find( MemberId upstream )
    {
        Optional<CatchupServerAddress> coreCatchupAddress = coreTopology.find( upstream ).map( a -> (CatchupServerAddress) a );
        Optional<CatchupServerAddress> readCatchupAddress = readReplicaTopology.find( upstream ).map( a -> (CatchupServerAddress) a );

        return coreCatchupAddress.map( Optional::of ).orElse( readCatchupAddress );
    }
}
