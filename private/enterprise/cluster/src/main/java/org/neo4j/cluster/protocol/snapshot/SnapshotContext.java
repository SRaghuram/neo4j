/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.snapshot;

import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerContext;
import org.neo4j.cluster.protocol.cluster.ClusterContext;

public class SnapshotContext
{
    private SnapshotProvider snapshotProvider;
    private ClusterContext clusterContext;
    private LearnerContext learnerContext;

    public SnapshotContext( ClusterContext clusterContext, LearnerContext learnerContext )
    {
        this.clusterContext = clusterContext;
        this.learnerContext = learnerContext;
    }

    public void setSnapshotProvider( SnapshotProvider snapshotProvider )
    {
        this.snapshotProvider = snapshotProvider;
    }

    public ClusterContext getClusterContext()
    {
        return clusterContext;
    }

    public LearnerContext getLearnerContext()
    {
        return learnerContext;
    }

    public SnapshotProvider getSnapshotProvider()
    {
        return snapshotProvider;
    }
}
