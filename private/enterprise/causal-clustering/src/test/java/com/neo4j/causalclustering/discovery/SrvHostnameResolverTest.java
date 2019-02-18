/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.SimpleLogService;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SrvHostnameResolverTest
{
    private final MockSrvRecordResolver mockSrvRecordResolver =
            new MockSrvRecordResolver( new HashMap<String,List<SrvRecordResolver.SrvRecord>>()
            {
                {
                    put( "emptyrecord.com", new ArrayList<>() );
                }
            } );

    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final AssertableLogProvider userLogProvider = new AssertableLogProvider();
    private final Config config = Config.defaults( CausalClusteringSettings.minimum_core_cluster_size_at_formation, "2" );

    private final SrvHostnameResolver resolver = new SrvHostnameResolver( new SimpleLogService( userLogProvider, logProvider ), mockSrvRecordResolver,
            config, RetryStrategyTest.testRetryStrategy( 1 ) );

    @Test
    public void hostnamesAndPortsAreResolvedByTheResolver()
    {
        // given
        mockSrvRecordResolver.addRecords( "_discovery._tcp.google.com",
                asList(
                        SrvRecordResolver.SrvRecord.parse( "1 1 80 1.2.3.4" ),
                        SrvRecordResolver.SrvRecord.parse( "1 1 8080 5.6.7.8" )
                )
        );

        // when
        Collection<AdvertisedSocketAddress> resolvedAddresses = resolver.resolve(
                new AdvertisedSocketAddress( "_discovery._tcp.google.com", 0 )
        );

        // then
        assertEquals( 2, resolvedAddresses.size() );

        assertTrue( resolvedAddresses.removeIf(
                address -> address.getHostname().equals( "1.2.3.4" ) && address.getPort() == 80
        ) );

        assertTrue( resolvedAddresses.removeIf(
                address -> address.getHostname().equals( "5.6.7.8" ) && address.getPort() == 8080
        ) );
    }

    @Test
    public void resolutionDetailsAreLoggedToUserLogs()
    {
        // given
        mockSrvRecordResolver.addRecord(
                "_resolutionDetailsAreLoggedToUserLogs._test.neo4j.com",
                SrvRecordResolver.SrvRecord.parse( "1 1 4321 1.2.3.4" )
        );

        // when
        resolver.resolve(
                new AdvertisedSocketAddress( "_resolutionDetailsAreLoggedToUserLogs._test.neo4j.com", 0 )
        );

        // then
        userLogProvider.assertContainsMessageContaining( "Resolved initial host '%s' to %s" );
    }

    @Test
    public void unknownHostExceptionsAreLoggedAsErrors()
    {
        // when
        resolver.resolve( new AdvertisedSocketAddress( "unknown.com", 0 ) );

        // then
        logProvider.assertContainsMessageContaining( "Failed to resolve srv records for '%s'" );
    }

    @Test
    public void emptyRecordListsAreLoggedAsErrors()
    {
        // when
        resolver.resolve( new AdvertisedSocketAddress( "emptyrecord.com", 0 ) );

        // then
        logProvider.assertContainsMessageContaining( "Failed to resolve srv records for '%s'" );
    }

    @Test
    public void resolverRetriesUntilHostnamesAreFound() throws Exception
    {
        // given
        mockSrvRecordResolver.addRecords( "_discovery._tcp.google.com",
                asList(
                        SrvRecordResolver.SrvRecord.parse( "1 1 80 1.2.3.4" ),
                        SrvRecordResolver.SrvRecord.parse( "1 1 8080 5.6.7.8" )
                )
        );
        SrvRecordResolver mockResolver = Mockito.spy( mockSrvRecordResolver );
        when(  mockResolver.resolveSrvRecord( anyString() ) )
                .thenReturn( Stream.empty() )
                .thenReturn( Stream.empty() )
                .thenCallRealMethod();

        SrvHostnameResolver resolver =
                new SrvHostnameResolver( new SimpleLogService( userLogProvider, logProvider ), mockResolver, config, RetryStrategyTest.testRetryStrategy( 2 ) );

        // when
        Collection<AdvertisedSocketAddress> resolvedAddresses = resolver.resolve(
                new AdvertisedSocketAddress( "_discovery._tcp.google.com", 0 )
        );

        // then
        verify( mockResolver, times( 3 ) ).resolveSrvRecord( "_discovery._tcp.google.com" );

        assertEquals( 2, resolvedAddresses.size() );

        assertTrue( resolvedAddresses.removeIf(
                address -> address.getHostname().equals( "1.2.3.4" ) && address.getPort() == 80
        ) );

        assertTrue( resolvedAddresses.removeIf(
                address -> address.getHostname().equals( "5.6.7.8" ) && address.getPort() == 8080
        ) );

    }
}
