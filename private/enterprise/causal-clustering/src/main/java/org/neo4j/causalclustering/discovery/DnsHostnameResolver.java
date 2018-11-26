/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

public class DnsHostnameResolver extends RetryingHostnameResolver
{
    private final Log userLog;
    private final Log log;
    private final DomainNameResolver domainNameResolver;

    public static RemoteMembersResolver resolver( LogService logService, DomainNameResolver domainNameResolver, Config config )
    {
        DnsHostnameResolver hostnameResolver =
                new DnsHostnameResolver( logService, domainNameResolver, config, defaultRetryStrategy( config ) );
        return new InitialDiscoveryMembersResolver( hostnameResolver, config );
    }

    DnsHostnameResolver( LogService logService, DomainNameResolver domainNameResolver, Config config, RetryStrategy retryStrategy )
    {
        super( config, retryStrategy );
        log = logService.getInternalLog( getClass() );
        userLog = logService.getUserLog( getClass() );
        this.domainNameResolver = domainNameResolver;
    }

    @Override
    protected Collection<AdvertisedSocketAddress> resolveOnce( AdvertisedSocketAddress initialAddress )
    {
        Set<AdvertisedSocketAddress> addresses = new HashSet<>();
        InetAddress[] ipAddresses;
        ipAddresses = domainNameResolver.resolveDomainName( initialAddress.getHostname() );
        if ( ipAddresses.length == 0 )
        {
            log.error( "Failed to resolve host '%s'", initialAddress.getHostname() );
        }

        for ( InetAddress ipAddress : ipAddresses )
        {
            addresses.add( new AdvertisedSocketAddress( ipAddress.getHostAddress(), initialAddress.getPort() ) );
        }

        userLog.info( "Resolved initial host '%s' to %s", initialAddress, addresses );
        return addresses;
    }

}
