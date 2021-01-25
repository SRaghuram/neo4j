/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import java.util.Objects;

public abstract class BaseProtocolResponse<IMPL extends Comparable<IMPL>> implements ClientMessage
{
    private final StatusCode statusCode;
    private final String protocolName;
    private final IMPL version;

    BaseProtocolResponse( StatusCode statusCode, String protocolName, IMPL version )
    {
        this.statusCode = statusCode;
        this.protocolName = protocolName;
        this.version = version;
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
        BaseProtocolResponse that = (BaseProtocolResponse) o;
        return Objects.equals( version, that.version ) && Objects.equals( protocolName, that.protocolName );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( protocolName, version );
    }

    public StatusCode statusCode()
    {
        return statusCode;
    }

    public String protocolName()
    {
        return protocolName;
    }

    public IMPL version()
    {
        return version;
    }

    @Override
    public String toString()
    {
        return "BaseProtocolResponse{" + "statusCode=" + statusCode + ", protocolName='" + protocolName + '\'' + ", version=" + version + '}';
    }
}
