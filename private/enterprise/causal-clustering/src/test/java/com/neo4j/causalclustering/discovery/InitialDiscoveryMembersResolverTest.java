/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;

import static com.neo4j.configuration.CausalClusteringSettings.initial_discovery_members;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InitialDiscoveryMembersResolverTest
{
    private HostnameResolver hostnameResolver = mock( HostnameResolver.class );

    private SocketAddress input1 = new SocketAddress( "foo.bar", 123 );
    private SocketAddress input2 = new SocketAddress( "baz.bar", 432 );
    private SocketAddress input3 = new SocketAddress( "quux.bar", 789 );

    private SocketAddress output1 = new SocketAddress( "a.b", 3 );
    private SocketAddress output2 = new SocketAddress( "b.b", 34 );
    private SocketAddress output3 = new SocketAddress( "c.b", 7 );

    private List<SocketAddress> inputAddresses = List.of( input1, input2, input3 );

    @Test
    void shouldReturnEmptyCollectionIfEmptyInitialMembers()
    {
        // given
        Config config = Config.defaults( initial_discovery_members, List.of() );

        InitialDiscoveryMembersResolver
                hostnameResolvingInitialDiscoveryMembersResolver = new InitialDiscoveryMembersResolver( hostnameResolver, config );

        // when
        Collection<SocketAddress> result = hostnameResolvingInitialDiscoveryMembersResolver.resolve( identity() );

        // then
        MatcherAssert.assertThat( result, empty() );
    }

    @Test
    void shouldResolveAndReturnAllConfiguredAddresses()
    {
        // given
        Config config = Config.defaults( initial_discovery_members, inputAddresses );

        when( hostnameResolver.resolve( input1 ) ).thenReturn( asList( output1, output2 ) );
        when( hostnameResolver.resolve( input2 ) ).thenReturn( emptyList() );
        when( hostnameResolver.resolve( input3 ) ).thenReturn( singletonList( output3 ) );

        InitialDiscoveryMembersResolver
                hostnameResolvingInitialDiscoveryMembersResolver = new InitialDiscoveryMembersResolver( hostnameResolver, config );

        // when
        Collection<SocketAddress> result = hostnameResolvingInitialDiscoveryMembersResolver.resolve( identity() );

        // then
        MatcherAssert.assertThat( result, containsInAnyOrder( output1, output2, output3 ) );
    }

    @Test
    void shouldDeDupeConfiguredAddresses()
    {
        // given
        Config config = Config.defaults( initial_discovery_members, inputAddresses );

        when( hostnameResolver.resolve( any() ) ).thenReturn( singletonList( output1 ) );

        InitialDiscoveryMembersResolver
                hostnameResolvingInitialDiscoveryMembersResolver = new InitialDiscoveryMembersResolver( hostnameResolver, config );

        // when
        Collection<SocketAddress> result = hostnameResolvingInitialDiscoveryMembersResolver.resolve( identity() );

        // then
        MatcherAssert.assertThat( result, contains( output1 ) );
    }

    /**
     * A consistent order of addresses appears to prevent some flakiness in integration tests. Presumably this would also prevent some flakiness in production
     * deployments.
     */
    @Test
    void shouldReturnConfiguredAddressesInOrder()
    {
        // given
        Config config = Config.defaults( initial_discovery_members, inputAddresses );

        when( hostnameResolver.resolve( any() ) )
                .thenReturn( singletonList( output3 ) )
                .thenReturn( singletonList( output1 ) )
                .thenReturn( singletonList( output2 ) );

        InitialDiscoveryMembersResolver
                hostnameResolvingInitialDiscoveryMembersResolver = new InitialDiscoveryMembersResolver( hostnameResolver, config );

        // when
        Collection<SocketAddress> result = hostnameResolvingInitialDiscoveryMembersResolver.resolve( identity() );

        // then
        MatcherAssert.assertThat( result, contains( output1, output2, output3 ) );
    }

    @Test
    void shouldApplyTransform()
    {
        // given
        SocketAddress input1 = new SocketAddress( "foo.bar", 123 );

        SocketAddress output1 = new SocketAddress( "a.b", 3 );

        Config config = Config.defaults( initial_discovery_members, List.of( input1 ) );

        when( hostnameResolver.resolve( input1 ) ).thenReturn( singletonList( output1 ) );

        InitialDiscoveryMembersResolver
                hostnameResolvingInitialDiscoveryMembersResolver = new InitialDiscoveryMembersResolver( hostnameResolver, config );

        // when
        Collection<String> result = hostnameResolvingInitialDiscoveryMembersResolver.resolve( address -> address.toString().toUpperCase() );

        // then
        MatcherAssert.assertThat( result, containsInAnyOrder( output1.toString().toUpperCase() ) );
    }
}
