/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.actor.Address;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.configuration.helpers.SocketAddress;

class JoinMessageFactoryTest
{
    private final List<Address> seenAddresses = Collections.singletonList( new Address( "protocol", "system", "host", 0 ) );

    @Test
    void shouldCreateMessageRequiringResolve()
    {
        // given
        JoinMessageFactory factory = new JoinMessageFactory( new NoOverrideRemoteMembersResolver() );

        // when
        JoinMessage message = factory.message();

        // then
        Assertions.assertTrue( message.isResolveRequired() );
    }

    @Test
    void shouldIgnoreSeenAddressWithNoOverrideResolver()
    {
        // given
        JoinMessageFactory factory = new JoinMessageFactory( new NoOverrideRemoteMembersResolver() );
        factory.addSeenAddresses( seenAddresses );

        // when
        JoinMessage message = factory.message();

        // then
        Assertions.assertFalse( message.hasAddress() );
    }

    @Test
    void shouldIncludeSeenAddressWithOverrideResolver()
    {
        // given
        JoinMessageFactory factory = new JoinMessageFactory( new OverrideRemoteMembersResolver() );
        factory.addSeenAddresses( seenAddresses );

        // when
        JoinMessage message = factory.message();

        // then
        Assertions.assertTrue( message.hasAddress() );
    }

    @Test
    void shouldCreateMessageWithEmptyHosts()
    {
        // given
        JoinMessageFactory factory = new JoinMessageFactory( new OverrideRemoteMembersResolver() );

        // when
        JoinMessage message = factory.message();

        // then
        Assertions.assertFalse( message.hasAddress() );
    }

    @Test
    void shouldClearHostsWhenCreatingMessage()
    {
        // given
        JoinMessageFactory factory = new JoinMessageFactory( new OverrideRemoteMembersResolver() );
        factory.addSeenAddresses( seenAddresses );

        // when
        JoinMessage message1 = factory.message();
        JoinMessage message2 = factory.message();

        // then
        Assertions.assertFalse( message2.hasAddress() );
    }

    private static class OverrideRemoteMembersResolver implements RemoteMembersResolver
    {
        @Override
        public <COLL extends Collection<REMOTE>, REMOTE> COLL resolve( Function<SocketAddress,REMOTE> transform, Supplier<COLL> collectionFactory )
        {
            return null;
        }

        @Override
        public boolean useOverrides()
        {
            return true;
        }

        @Override
        public Optional<SocketAddress> first()
        {
            return Optional.empty();
        }
    }

    private static class NoOverrideRemoteMembersResolver implements RemoteMembersResolver
    {

        @Override
        public <COLL extends Collection<REMOTE>, REMOTE> COLL resolve( Function<SocketAddress,REMOTE> transform, Supplier<COLL> collectionFactory )
        {
            return null;
        }

        @Override
        public boolean useOverrides()
        {
            return false;
        }

        @Override
        public Optional<SocketAddress> first()
        {
            return Optional.empty();
        }
    }
}
