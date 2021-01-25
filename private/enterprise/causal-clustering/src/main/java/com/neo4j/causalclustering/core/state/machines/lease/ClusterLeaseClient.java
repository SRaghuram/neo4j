/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.lease;

import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.api.LeaseException;

import static org.neo4j.kernel.api.exceptions.Status.Cluster.NotALeader;
import static org.neo4j.kernel.impl.api.LeaseService.NO_LEASE;

public class ClusterLeaseClient implements LeaseClient
{
    private final ClusterLeaseCoordinator coordinator;

    private int leaseId = NO_LEASE;

    ClusterLeaseClient( ClusterLeaseCoordinator coordinator )
    {
        this.coordinator = coordinator;
    }

    @Override
    public int leaseId()
    {
        return leaseId;
    }

    /**
     * This ensures that a valid lease was held at some point in time. It throws an
     * exception if it was held but was later lost or never could be taken to
     * begin with.
     */
    @Override
    public void ensureValid() throws LeaseException
    {
        if ( leaseId == NO_LEASE )
        {
            leaseId = coordinator.acquireLeaseOrThrow();
        }
        else if ( coordinator.isInvalid( leaseId ) )
        {
            throw new LeaseException( "Local instance lost lease.", NotALeader );
        }
    }
}
