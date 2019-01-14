/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.cluster;

import java.net.URI;

import org.neo4j.cluster.InstanceId;

/**
 * Context used by the {@link HighAvailabilityMemberStateMachine}. Keeps track of what elections and previously
 * available master this cluster member has seen.
 */
public class SimpleHighAvailabilityMemberContext implements HighAvailabilityMemberContext
{
    private InstanceId electedMasterId;
    private URI availableHaMasterId;
    private final InstanceId myId;
    private boolean slaveOnly;

    public SimpleHighAvailabilityMemberContext( InstanceId myId, boolean slaveOnly )
    {
        this.myId = myId;
        this.slaveOnly = slaveOnly;
    }

    @Override
    public InstanceId getMyId()
    {
        return myId;
    }

    @Override
    public InstanceId getElectedMasterId()
    {
        return electedMasterId;
    }

    @Override
    public void setElectedMasterId( InstanceId electedMasterId )
    {
        this.electedMasterId = electedMasterId;
    }

    @Override
    public URI getAvailableHaMaster()
    {
        return availableHaMasterId;
    }

    @Override
    public void setAvailableHaMasterId( URI availableHaMasterId )
    {
        this.availableHaMasterId = availableHaMasterId;
    }

    @Override
    public boolean isSlaveOnly()
    {
        return slaveOnly;
    }

    @Override
    public String toString()
    {
        return "SimpleHighAvailabilityMemberContext{" +
                "electedMasterId=" + electedMasterId +
                ", availableHaMasterId=" + availableHaMasterId +
                ", myId=" + myId +
                ", slaveOnly=" + slaveOnly +
                '}';
    }
}
