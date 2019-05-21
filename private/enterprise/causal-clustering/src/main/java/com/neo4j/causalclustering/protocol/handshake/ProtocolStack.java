/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.protocol.Protocol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.stream.Collectors.toList;

public class ProtocolStack
{
    private final Protocol.ApplicationProtocol applicationProtocol;
    private final List<Protocol.ModifierProtocol> modifierProtocols;

    public ProtocolStack( Protocol.ApplicationProtocol applicationProtocol, List<Protocol.ModifierProtocol> modifierProtocols )
    {
        this.applicationProtocol = applicationProtocol;
        this.modifierProtocols = Collections.unmodifiableList( modifierProtocols );
    }

    public Protocol.ApplicationProtocol applicationProtocol()
    {
        return applicationProtocol;
    }

    public List<Protocol.ModifierProtocol> modifierProtocols()
    {
        return modifierProtocols;
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
        ProtocolStack that = (ProtocolStack) o;
        return Objects.equals( applicationProtocol, that.applicationProtocol ) && Objects.equals( modifierProtocols, that.modifierProtocols );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( applicationProtocol, modifierProtocols );
    }

    @Override
    public String toString()
    {
        String desc = format( "%s version:%s", applicationProtocol.category(), applicationProtocol.implementation() );
        List<String> modifierNames = modifierProtocols.stream().map( Protocol::implementation ).collect( toList() );

        if ( !modifierNames.isEmpty() )
        {
            desc = format( "%s (%s)", desc, join( ", ", modifierNames ) );
        }

        return desc;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Protocol.ApplicationProtocol applicationProtocol;
        private final List<Protocol.ModifierProtocol> modifierProtocols = new ArrayList<>();

        private Builder()
        {
        }

        public Builder modifier( Protocol.ModifierProtocol modifierProtocol )
        {
            modifierProtocols.add( modifierProtocol );
            return this;
        }

        public Builder application( Protocol.ApplicationProtocol applicationProtocol )
        {
            this.applicationProtocol = applicationProtocol;
            return this;
        }

        ProtocolStack build()
        {
            return new ProtocolStack( applicationProtocol, modifierProtocols );
        }

        @Override
        public String toString()
        {
            return "Builder{" + "applicationProtocol=" + applicationProtocol + ", modifierProtocols=" + modifierProtocols + '}';
        }
    }
}
