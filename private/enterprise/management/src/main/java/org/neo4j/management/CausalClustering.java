/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management;

import org.neo4j.jmx.Description;
import org.neo4j.jmx.ManagementInterface;

@ManagementInterface( name = CausalClustering.NAME )
@Description( "Information about an instance participating in a causal cluster" )
public interface CausalClustering
{
    String NAME = "Causal Clustering";

    @Description( "The current role this member has in the cluster" )
    String getRole();

    @Description( "The total amount of disk space used by the raft log, in bytes" )
    long getRaftLogSize();

    @Description( "The total amount of disk space used by the replicated states, in bytes" )
    long getReplicatedStateSize();
}
