/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import java.util.Objects;
import java.util.Set;

public abstract class BaseProtocolRequest<IMPL extends Comparable<IMPL>> implements ServerMessage
{
    private final String protocolName;
    private final Set<IMPL> versions;

    BaseProtocolRequest( String protocolName, Set<IMPL> versions )
    {
        this.protocolName = protocolName;
        this.versions = versions;
    }

    public String protocolName()
    {
        return protocolName;
    }

    public Set<IMPL> versions()
    {
        return versions;
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
        BaseProtocolRequest that = (BaseProtocolRequest) o;
        return Objects.equals( protocolName, that.protocolName ) && Objects.equals( versions, that.versions );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( protocolName, versions );
    }

    @Override
    public String toString()
    {
        return "BaseProtocolRequest{" + "protocolName='" + protocolName + '\'' + ", versions=" + versions + '}';
    }
}
