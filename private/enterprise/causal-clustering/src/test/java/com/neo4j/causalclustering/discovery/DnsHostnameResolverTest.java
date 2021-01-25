/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.configuration.CausalClusteringSettings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.SimpleLogService;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.LogAssertions.assertThat;

class DnsHostnameResolverTest
{
    private final MapDomainNameResolver mockDomainNameResolver = new MapDomainNameResolver( new HashMap<>() );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final AssertableLogProvider userLogProvider = new AssertableLogProvider();
    private final Config config = Config.defaults( CausalClusteringSettings.minimum_core_cluster_size_at_formation, 2 );

    private final DnsHostnameResolver resolver = new DnsHostnameResolver( new SimpleLogService( userLogProvider, logProvider ), mockDomainNameResolver,
            config, RetryStrategyTest.testRetryStrategy( 1 ) );

    @Test
    void hostnamesAreResolvedByTheResolver()
    {
        // given
        mockDomainNameResolver.setHostnameAddresses( "google.com", asList( "1.2.3.4", "5.6.7.8" ) );

        // when
        Collection<SocketAddress> resolvedAddresses =
                resolver.resolve( new SocketAddress( "google.com", 80 ) );

        // then
        Assertions.assertEquals( 2, resolvedAddresses.size() );
        Assertions.assertTrue( resolvedAddresses.removeIf( address -> address.getHostname().equals( "1.2.3.4" ) ) );
        Assertions.assertTrue( resolvedAddresses.removeIf( address -> address.getHostname().equals( "5.6.7.8" ) ) );
    }

    @Test
    void resolvedHostnamesUseTheSamePort()
    {
        // given
        mockDomainNameResolver.setHostnameAddresses( "google.com", asList( "1.2.3.4", "5.6.7.8" ) );

        // when
        List<SocketAddress> resolvedAddresses =
                new ArrayList<>( resolver.resolve( new SocketAddress( "google.com", 1234 ) ) );

        // then
        Assertions.assertEquals( 2, resolvedAddresses.size() );
        Assertions.assertEquals( 1234, resolvedAddresses.get( 0 ).getPort() );
        Assertions.assertEquals( 1234, resolvedAddresses.get( 1 ).getPort() );
    }

    @Test
    void resolutionDetailsAreLoggedToUserLogs()
    {
        // given
        mockDomainNameResolver.setHostnameAddresses( "google.com", asList( "1.2.3.4", "5.6.7.8" ) );

        // when
        resolver.resolve( new SocketAddress( "google.com", 1234 ) );

        // then
        assertThat( userLogProvider ).containsMessages( "Resolved initial host '%s' to %s" );
    }

    @Test
    void unknownHostExceptionsAreLoggedAsErrors()
    {
        // when
        resolver.resolve( new SocketAddress( "google.com", 1234 ) );

        // then
        assertThat( logProvider ).containsMessages( "Failed to resolve host '%s'" );
    }

    @Test
    void resolverRetriesUntilHostnamesAreFound()
    {
        // given
        mockDomainNameResolver.setHostnameAddresses( "google.com", asList( "1.2.3.4", "5.6.7.8" ) );
        DomainNameResolver mockResolver = spy( mockDomainNameResolver );
        when( mockResolver.resolveDomainName( anyString() ) )
                .thenReturn( new InetAddress[0] )
                .thenReturn( new InetAddress[0] )
                .thenCallRealMethod();

        DnsHostnameResolver resolver =
                new DnsHostnameResolver( new SimpleLogService( userLogProvider, logProvider ), mockResolver, config, RetryStrategyTest.testRetryStrategy( 2 ) );

        // when
        List<SocketAddress> resolvedAddresses =
                new ArrayList<>( resolver.resolve( new SocketAddress( "google.com", 1234 ) ) );

        // then
        verify( mockResolver, times( 3 ) ).resolveDomainName( "google.com" );
        Assertions.assertEquals( 2, resolvedAddresses.size() );
        Assertions.assertEquals( 1234, resolvedAddresses.get( 0 ).getPort() );
        Assertions.assertEquals( 1234, resolvedAddresses.get( 1 ).getPort() );
    }
}
