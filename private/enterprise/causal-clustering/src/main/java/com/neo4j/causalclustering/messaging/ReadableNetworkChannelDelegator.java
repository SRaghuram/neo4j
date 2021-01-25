/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import java.io.IOException;

import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChecksumChannel;

import static java.util.Objects.requireNonNull;

public class ReadableNetworkChannelDelegator implements ReadableClosablePositionAwareChecksumChannel
{
    private ReadableClosablePositionAwareChecksumChannel delegate;

    public void delegateTo( ReadableClosablePositionAwareChecksumChannel channel )
    {
        this.delegate = requireNonNull( channel );
    }

    @Override
    public LogPositionMarker getCurrentPosition( LogPositionMarker positionMarker ) throws IOException
    {
        assertAssigned();
        return delegate.getCurrentPosition( positionMarker );
    }

    @Override
    public LogPosition getCurrentPosition() throws IOException
    {
        assertAssigned();
        return delegate.getCurrentPosition();
    }

    @Override
    public byte get() throws IOException
    {
        assertAssigned();
        return delegate.get();
    }

    @Override
    public short getShort() throws IOException
    {
        assertAssigned();
        return delegate.getShort();
    }

    @Override
    public int getInt() throws IOException
    {
        assertAssigned();
        return delegate.getInt();
    }

    @Override
    public long getLong() throws IOException
    {
        assertAssigned();
        return delegate.getLong();
    }

    @Override
    public float getFloat() throws IOException
    {
        assertAssigned();
        return delegate.getFloat();
    }

    @Override
    public double getDouble() throws IOException
    {
        assertAssigned();
        return delegate.getDouble();
    }

    @Override
    public void get( byte[] bytes, int length ) throws IOException
    {
        assertAssigned();
        delegate.get( bytes, length );
    }

    @Override
    public void close() throws IOException
    {
        assertAssigned();
        delegate.close();
    }

    private void assertAssigned()
    {
        if ( delegate == null )
        {
            throw new IllegalArgumentException( "No assigned channel to delegate reads" );
        }
    }

    @Override
    public void beginChecksum()
    {
        // no op
    }

    @Override
    public int endChecksumAndValidate() throws IOException
    {
        return 0; // no op
    }
}
