/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

public interface PullRequestMonitor
{
    void txPullRequest( long txId );
    void txPullResponse( long txId );
    long lastRequestedTxId();
    long lastReceivedTxId();
    long numberOfRequests();
}
