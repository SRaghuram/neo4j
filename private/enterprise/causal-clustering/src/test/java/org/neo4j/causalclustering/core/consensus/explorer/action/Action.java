/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.explorer.action;

import org.neo4j.causalclustering.core.consensus.explorer.ClusterState;

public interface Action
{
    ClusterState advance( ClusterState clusterState );
}
