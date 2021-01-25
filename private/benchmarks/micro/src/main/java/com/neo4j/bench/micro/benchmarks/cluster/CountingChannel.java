/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster;

import org.neo4j.io.fs.WritableChannel;

class CountingChannel implements WritableChannel
{
    int totalSize;

    @Override
    public WritableChannel put( byte value )
    {
        totalSize++;
        return this;
    }

    @Override
    public WritableChannel putShort( short value )
    {
        totalSize += Short.BYTES;
        return this;
    }

    @Override
    public WritableChannel putInt( int value )
    {
        totalSize += Integer.BYTES;
        return this;
    }

    @Override
    public WritableChannel putLong( long value )
    {
        totalSize += Long.BYTES;
        return this;
    }

    @Override
    public WritableChannel putFloat( float value )
    {
        totalSize += Float.BYTES;
        return this;
    }

    @Override
    public WritableChannel putDouble( double value )
    {
        totalSize += Double.BYTES;
        return this;
    }

    @Override
    public WritableChannel put( byte[] value, int length )
    {
        totalSize += length;
        return this;
    }
}
