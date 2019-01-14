/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.io.Serializable;

import org.neo4j.cluster.com.message.Message;

/**
 * Id of a particular Paxos instance.
 */
public class InstanceId
        implements Serializable, Comparable<InstanceId>
{
    private static final long serialVersionUID = 2505002855546341672L;

    public static final String INSTANCE = "instance";

    long id;

    public InstanceId( Message message )
    {
        this( message.getHeader( INSTANCE ) );
    }

    public InstanceId( String string )
    {
        this( Long.parseLong( string ) );
    }

    public InstanceId( long id )
    {
        this.id = id;
    }

    public long getId()
    {
        return id;
    }

    @Override
    public int compareTo( InstanceId o )
    {
        return Long.compare( id, o.getId() );
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

        InstanceId that = (InstanceId) o;

        return id == that.id;
    }

    @Override
    public int hashCode()
    {
        return (int) (id ^ (id >>> 32));
    }

    @Override
    public String toString()
    {
        return Long.toString( id );
    }
}
