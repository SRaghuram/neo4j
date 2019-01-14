/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

public final class ProtocolVersion implements Comparable<ProtocolVersion>
{
    public static final byte INTERNAL_PROTOCOL_VERSION = 2;

    private final byte applicationProtocol;
    private final byte internalProtocol;

    public ProtocolVersion( byte applicationProtocol, byte internalProtocol )
    {
        this.applicationProtocol = applicationProtocol;
        this.internalProtocol = internalProtocol;
    }

    public byte getApplicationProtocol()
    {
        return applicationProtocol;
    }

    public byte getInternalProtocol()
    {
        return internalProtocol;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj == null )
        {
            return false;
        }
        if ( obj.getClass() != ProtocolVersion.class )
        {
            return false;
        }
        ProtocolVersion other = (ProtocolVersion) obj;
        return (other.applicationProtocol == applicationProtocol) && (other.internalProtocol == internalProtocol);
    }

    @Override
    public int hashCode()
    {
        return (31 * applicationProtocol) | internalProtocol;
    }

    @Override
    public int compareTo( ProtocolVersion that )
    {
        return Byte.compare( this.applicationProtocol, that.applicationProtocol );
    }

    @Override
    public String toString()
    {
        return "ProtocolVersion{" +
                "applicationProtocol=" + applicationProtocol +
                ", internalProtocol=" + internalProtocol +
                '}';
    }
}
