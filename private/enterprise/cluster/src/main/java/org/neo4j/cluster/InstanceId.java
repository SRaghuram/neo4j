/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;

import static org.neo4j.helpers.Uris.parameter;

/**
 * Represents the concept of the cluster wide unique id of an instance. The
 * main requirement is total order over the instances of the class, currently
 * implemented by an encapsulated integer.
 * It is also expected to be serializable, as it's transmitted over the wire
 * as part of messages between instances.
 */
public class InstanceId implements Externalizable, Comparable<InstanceId>
{
    public static final InstanceId NONE = new InstanceId( Integer.MIN_VALUE );

    private int serverId;

    public InstanceId()
    {}

    public InstanceId( int serverId )
    {
        this.serverId = serverId;
    }

    @Override
    public int compareTo( InstanceId o )
    {
        return Integer.compare( serverId, o.serverId );
    }

    @Override
    public int hashCode()
    {
        return serverId;
    }

    @Override
    public String toString()
    {
        return Integer.toString( serverId );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }

        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        InstanceId instanceId1 = (InstanceId) o;

        return serverId == instanceId1.serverId;
    }

    @Override
    public void writeExternal( ObjectOutput out ) throws IOException
    {
        out.writeInt( serverId );
    }

    @Override
    public void readExternal( ObjectInput in ) throws IOException
    {
        serverId = in.readInt();
    }

    public int toIntegerIndex()
    {
        return serverId;
    }

    public String instanceNameFromURI( URI member )
    {
        String name = member == null ? null : parameter( "memberName" ).apply( member );
        return name == null ? toString() : name;
    }
}
