/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.initial_discovery_members;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class InitialDiscoveryMembersResolverTest
{
    @Mock
    private HostnameResolver hostnameResolver;

    private AdvertisedSocketAddress input1 = new AdvertisedSocketAddress( "foo.bar", 123 );
    private AdvertisedSocketAddress input2 = new AdvertisedSocketAddress( "baz.bar", 432 );
    private AdvertisedSocketAddress input3 = new AdvertisedSocketAddress( "quux.bar", 789 );

    private AdvertisedSocketAddress output1 = new AdvertisedSocketAddress( "a.b", 3 );
    private AdvertisedSocketAddress output2 = new AdvertisedSocketAddress( "b.b", 34 );
    private AdvertisedSocketAddress output3 = new AdvertisedSocketAddress( "c.b", 7 );

    private String configString = Stream.of( input1, input2, input3 ).map( AdvertisedSocketAddress::toString ).collect( Collectors.joining( "," ) );

    @Test
    public void shouldReturnEmptyCollectionIfEmptyInitialMembers()
    {
        // given
        Config config = Config.builder()
                .withSetting( initial_discovery_members, "" )
                .build();

        InitialDiscoveryMembersResolver
                hostnameResolvingInitialDiscoveryMembersResolver = new InitialDiscoveryMembersResolver( hostnameResolver, config );

        // when
        Collection<AdvertisedSocketAddress> result = hostnameResolvingInitialDiscoveryMembersResolver.resolve( identity() );

        // then
        assertThat( result, empty() );
    }

    @Test
    public void shouldResolveAndReturnAllConfiguredAddresses()
    {
        // given
        Config config = Config.builder()
                .withSetting( initial_discovery_members, configString )
                .build();

        when( hostnameResolver.resolve( input1 ) ).thenReturn( asList( output1, output2 ) );
        when( hostnameResolver.resolve( input2 ) ).thenReturn( emptyList() );
        when( hostnameResolver.resolve( input3 ) ).thenReturn( singletonList( output3 ) );

        InitialDiscoveryMembersResolver
                hostnameResolvingInitialDiscoveryMembersResolver = new InitialDiscoveryMembersResolver( hostnameResolver, config );

        // when
        Collection<AdvertisedSocketAddress> result = hostnameResolvingInitialDiscoveryMembersResolver.resolve( identity() );

        // then
        assertThat( result, containsInAnyOrder( output1, output2, output3 ) );
    }

    @Test
    public void shouldDeDupeConfiguredAddresses()
    {
        // given
        Config config = Config.builder()
                .withSetting( initial_discovery_members, configString )
                .build();

        when( hostnameResolver.resolve( any() ) ).thenReturn( singletonList( output1 ) );

        InitialDiscoveryMembersResolver
                hostnameResolvingInitialDiscoveryMembersResolver = new InitialDiscoveryMembersResolver( hostnameResolver, config );

        // when
        Collection<AdvertisedSocketAddress> result = hostnameResolvingInitialDiscoveryMembersResolver.resolve( identity() );

        // then
        assertThat( result, contains( output1 ) );
    }

    /**
     * A consistent order of addresses appears to prevent some flakiness in integration tests. Presumably this would also prevent some flakiness in
     * production deployments.
     */
    @Test
    public void shouldReturnConfiguredAddressesInOrder()
    {
        // given
        Config config = Config.builder()
                .withSetting( initial_discovery_members, configString )
                .build();

        when( hostnameResolver.resolve( any() ) )
                .thenReturn( singletonList( output3 ) )
                .thenReturn( singletonList( output1 ) )
                .thenReturn( singletonList( output2  ));

        InitialDiscoveryMembersResolver
                hostnameResolvingInitialDiscoveryMembersResolver = new InitialDiscoveryMembersResolver( hostnameResolver, config );

        // when
        Collection<AdvertisedSocketAddress> result = hostnameResolvingInitialDiscoveryMembersResolver.resolve( identity() );

        // then
        assertThat( result, contains( output1, output2, output3 ) );
    }

    @Test
    public void shouldApplyTransform()
    {
        // given
        AdvertisedSocketAddress input1 = new AdvertisedSocketAddress( "foo.bar", 123 );

        AdvertisedSocketAddress output1 = new AdvertisedSocketAddress( "a.b", 3 );

        Config config = Config.builder()
                .withSetting( initial_discovery_members, input1.toString() )
                .build();

        when( hostnameResolver.resolve( input1 ) ).thenReturn( singletonList( output1 ) );

        InitialDiscoveryMembersResolver
                hostnameResolvingInitialDiscoveryMembersResolver = new InitialDiscoveryMembersResolver( hostnameResolver, config );

        // when
        Collection<String> result = hostnameResolvingInitialDiscoveryMembersResolver.resolve( address -> address.toString().toUpperCase() );

        // then
        assertThat( result, containsInAnyOrder( output1.toString().toUpperCase() ) );
    }
}
