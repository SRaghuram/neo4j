/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.configuration.ApplicationProtocolVersion;

import java.util.List;
import java.util.Objects;

import org.neo4j.internal.helpers.collection.Pair;

public class SwitchOverRequest implements ServerMessage
{
    static final int MESSAGE_CODE = 3;

    private final String protocolName;
    private final ApplicationProtocolVersion version;
    private final List<Pair<String,String>> modifierProtocols;

    public SwitchOverRequest( String applicationProtocolName, ApplicationProtocolVersion applicationProtocolVersion,
            List<Pair<String,String>> modifierProtocols )
    {
        this.protocolName = applicationProtocolName;
        this.version = applicationProtocolVersion;
        this.modifierProtocols = modifierProtocols;
    }

    @Override
    public void dispatch( ServerMessageHandler handler )
    {
        handler.handle( this );
    }

    public String protocolName()
    {
        return protocolName;
    }

    public List<Pair<String,String>> modifierProtocols()
    {
        return modifierProtocols;
    }

    public ApplicationProtocolVersion version()
    {
        return version;
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
        SwitchOverRequest that = (SwitchOverRequest) o;
        return Objects.equals( version, that.version ) &&
                Objects.equals( protocolName, that.protocolName ) &&
                Objects.equals( modifierProtocols, that.modifierProtocols );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( protocolName, version, modifierProtocols );
    }

    @Override
    public String toString()
    {
        return "SwitchOverRequest{" + "protocolName='" + protocolName + '\'' + ", version=" + version + ", modifierProtocols=" + modifierProtocols + '}';
    }
}
