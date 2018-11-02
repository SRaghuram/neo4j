/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.com.master;

import org.neo4j.com.ComException;

public class InvalidEpochException extends ComException
{
    private final long correctEpoch;
    private final long invalidEpoch;

    public InvalidEpochException( long correctEpoch, long invalidEpoch )
    {
        super( "Invalid epoch " + invalidEpoch + ", correct epoch is " + correctEpoch );
        this.correctEpoch = correctEpoch;
        this.invalidEpoch = invalidEpoch;
    }

    @Override
    public String toString()
    {
        return "InvalidEpochException{correctEpoch=" + correctEpoch + ", invalidEpoch=" + invalidEpoch + "}";
    }
}
