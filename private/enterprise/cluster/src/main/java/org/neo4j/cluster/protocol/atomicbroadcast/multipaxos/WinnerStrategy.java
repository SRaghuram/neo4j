/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.util.Collection;

/**
 * Strategy to be used to select a winner in a cluster coordinator election.
 */
public interface WinnerStrategy
{
    org.neo4j.cluster.InstanceId pickWinner( Collection<Vote> votes );
}
