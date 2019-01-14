/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.election;

import java.io.Externalizable;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Election credentials stating that this instance cannot be elected. The vote still counts towards the total though.
 */
public final class NotElectableElectionCredentials implements ElectionCredentials, Externalizable
{
    // For Externalizable
    public NotElectableElectionCredentials()
    {}

    @Override
    public int compareTo( ElectionCredentials o )
    {
        return -1;
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj != null && obj instanceof NotElectableElectionCredentials;
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    @Override
    public void writeExternal( ObjectOutput out )
    {
    }

    @Override
    public void readExternal( ObjectInput in )
    {
    }
}
