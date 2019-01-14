/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.cluster;

import java.net.URI;

import org.neo4j.cluster.InstanceId;

/**
 * This event represents a change in the cluster members internal state. The possible states
 * are enumerated in {@link HighAvailabilityMemberState}.
 */
public class HighAvailabilityMemberChangeEvent
{
    private final HighAvailabilityMemberState oldState;
    private final HighAvailabilityMemberState newState;
    private final InstanceId instanceId;
    private final URI serverHaUri;

    public HighAvailabilityMemberChangeEvent( HighAvailabilityMemberState oldState,
                                              HighAvailabilityMemberState newState,
                                              InstanceId instanceId, URI serverHaUri )
    {
        this.oldState = oldState;
        this.newState = newState;
        this.instanceId = instanceId;
        this.serverHaUri = serverHaUri;
    }

    public HighAvailabilityMemberState getOldState()
    {
        return oldState;
    }

    public HighAvailabilityMemberState getNewState()
    {
        return newState;
    }

    public InstanceId getInstanceId()
    {
        return instanceId;
    }

    public URI getServerHaUri()
    {
        return serverHaUri;
    }

    @Override
    public String toString()
    {
        return "HA Member State Event[ old state: " + oldState + ", new state: " + newState +
                ", server cluster URI: " + instanceId + ", server HA URI: " + serverHaUri + "]";
    }
}
