/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.com.master;

public class HandshakeResult
{
    private final long txChecksum;
    private final long epoch;

    public HandshakeResult( long txChecksum, long epoch )
    {
        this.txChecksum = txChecksum;
        this.epoch = epoch;
    }

    public long epoch()
    {
        return epoch;
    }

    public long txChecksum()
    {
        return txChecksum;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[checksum:" + txChecksum + ", epoch:" + epoch + "]";
    }
}
