/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.protocol.handshake;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.neo4j.causalclustering.protocol.Protocol;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.stream.Collectors.toList;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocol;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocol;

public class ProtocolStack
{
    private final ApplicationProtocol applicationProtocol;
    private final List<ModifierProtocol> modifierProtocols;

    public ProtocolStack( ApplicationProtocol applicationProtocol, List<ModifierProtocol> modifierProtocols )
    {
        this.applicationProtocol = applicationProtocol;
        this.modifierProtocols = Collections.unmodifiableList( modifierProtocols );
    }

    public ApplicationProtocol applicationProtocol()
    {
        return applicationProtocol;
    }

    public List<ModifierProtocol> modifierProtocols()
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
        String desc = format( "%s version:%d", applicationProtocol.category(), applicationProtocol.implementation() );
        List<String> modifierNames = modifierProtocols.stream().map( Protocol::implementation ).collect( toList() );

        if ( modifierNames.size() > 0 )
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
        private ApplicationProtocol applicationProtocol;
        private final List<ModifierProtocol> modifierProtocols = new ArrayList<>();

        private Builder()
        {
        }

        public Builder modifier( ModifierProtocol modifierProtocol )
        {
            modifierProtocols.add( modifierProtocol );
            return this;
        }

        public Builder application( ApplicationProtocol applicationProtocol )
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
