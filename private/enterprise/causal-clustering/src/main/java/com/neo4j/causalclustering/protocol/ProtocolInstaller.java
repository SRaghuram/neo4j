/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol;

import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;
import io.netty.channel.Channel;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public interface ProtocolInstaller<O extends ProtocolInstaller.Orientation>
{
    abstract class Factory<O extends ProtocolInstaller.Orientation, I extends ProtocolInstaller<O>>
    {
        private final ApplicationProtocol applicationProtocol;
        private final Function<List<ModifierProtocolInstaller<O>>,I> constructor;

        protected Factory( ApplicationProtocol applicationProtocol, Function<List<ModifierProtocolInstaller<O>>,I> constructor )
        {
            this.applicationProtocol = applicationProtocol;
            this.constructor = constructor;
        }

        I create( List<ModifierProtocolInstaller<O>> modifiers )
        {
            return constructor.apply( modifiers );
        }

        public ApplicationProtocol applicationProtocol()
        {
            return applicationProtocol;
        }
    }

    void install( Channel channel ) throws Exception;

    /**
     * For testing
     */
    Collection<Collection<ModifierProtocol>> modifiers();

    interface Orientation
    {
        interface Server extends Orientation
        {
            String INBOUND = "inbound";
        }

        interface Client extends Orientation
        {
            String OUTBOUND = "outbound";
        }
    }
}
