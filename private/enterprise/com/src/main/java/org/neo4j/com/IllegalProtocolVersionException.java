/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

/**
 * Thrown when a communication between client/server is attempted and either of internal protocol version
 * and application protocol doesn't match.
 *
 * @author Mattias Persson
 */
public class IllegalProtocolVersionException extends ComException
{
    private final byte expected;
    private final byte received;

    public IllegalProtocolVersionException( byte expected, byte received, String message )
    {
        super( message );
        this.expected = expected;
        this.received = received;
    }

    public byte getExpected()
    {
        return expected;
    }

    public byte getReceived()
    {
        return received;
    }

    @Override
    public String toString()
    {
        return "IllegalProtocolVersionException{expected=" + expected + ", received=" + received + "}";
    }
}
