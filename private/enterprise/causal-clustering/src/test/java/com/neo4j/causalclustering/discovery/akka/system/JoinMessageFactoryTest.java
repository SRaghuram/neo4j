/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.actor.Address;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.configuration.helpers.SocketAddress;

class JoinMessageFactoryTest
{
    private final List<Address> seenAddresses = Collections.singletonList( new Address( "protocol", "system", "host", 0 ) );

    @Test
    void shouldCreateMessageNotRejoin()
    {
        // given
        JoinMessageFactory factory = new JoinMessageFactory( new NoOverrideRemoteMembersResolver() );

        // when
        JoinMessage message = factory.message();

        // then
        Assertions.assertFalse( message.isReJoin() );
    }

    @Test
    void shouldCreateReJoinMessageAfterAddingSeenAddressWithNoOverrideResolver()
    {
        // given
        JoinMessageFactory factory = new JoinMessageFactory( new NoOverrideRemoteMembersResolver() );
        factory.addSeenAddresses( seenAddresses );

        // when
        JoinMessage message = factory.message();

        // then
        Assertions.assertTrue( message.isReJoin() );
    }

    @Test
    void shouldCreateReJoinMessageAfterAddingSeenAddressWithOverrideResolver()
    {
        // given
        JoinMessageFactory factory = new JoinMessageFactory( new OverrideRemoteMembersResolver() );
        factory.addSeenAddresses( seenAddresses );

        // when
        JoinMessage message = factory.message();

        // then
        Assertions.assertTrue( message.isReJoin() );
    }

    @Test
    void shouldCreateMessageWithEmptyHostsIfNotReJoin()
    {
        // given
        JoinMessageFactory factory = new JoinMessageFactory( new OverrideRemoteMembersResolver() );

        // when
        JoinMessage message = factory.message();

        // then
        Assertions.assertFalse( message.hasAddress() );
    }

    @Test
    void shouldCreateMessageWithEmptyHostsIfReJoinNoOverrideResolver()
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
    void shouldCreateMessageWithNonEmptyHostsIfReJoinOverrideResolver()
    {
        // given
        JoinMessageFactory factory = new JoinMessageFactory( new OverrideRemoteMembersResolver() );
        factory.addSeenAddresses( seenAddresses );

        // when
        JoinMessage message = factory.message();

        // then
        Assertions.assertTrue( message.hasAddress() );
        MatcherAssert.assertThat( message.head(), Matchers.notNullValue() );
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
    }
}
