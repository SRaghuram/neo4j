/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;

public interface TopologyUpdateSink
{
    void onTopologyUpdate( DatabaseCoreTopology topology );

    void onTopologyUpdate( DatabaseReadReplicaTopology topology );
}
