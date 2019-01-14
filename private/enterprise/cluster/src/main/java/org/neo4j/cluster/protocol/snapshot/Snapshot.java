/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.snapshot;

/**
 * API for getting a snapshot of current state from an instance in a cluster.
 * Mainly used when a new instance joins, and wants to catch up without having
 * to re-read all previous messages.
 */
public interface Snapshot
{
    void setSnapshotProvider( SnapshotProvider snapshotProvider );

    void refreshSnapshot();
}
