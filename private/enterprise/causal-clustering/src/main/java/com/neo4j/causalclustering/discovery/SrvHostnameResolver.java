/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import javax.naming.NamingException;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

public class SrvHostnameResolver extends RetryingHostnameResolver
{
    private final Log userLog;
    private final Log log;
    private final SrvRecordResolver srvRecordResolver;

    public static RemoteMembersResolver resolver( LogService logService, SrvRecordResolver srvHostnameResolver, Config config )
    {
        SrvHostnameResolver hostnameResolver = new SrvHostnameResolver( logService, srvHostnameResolver, config, defaultRetryStrategy( config ) );
        return new InitialDiscoveryMembersResolver( hostnameResolver, config );
    }

    SrvHostnameResolver( LogService logService, SrvRecordResolver srvRecordResolver, Config config,
            RetryStrategy retryStrategy )
    {
        super( config, retryStrategy );
        log = logService.getInternalLog( getClass() );
        userLog = logService.getUserLog( getClass() );
        this.srvRecordResolver = srvRecordResolver;
    }

    @Override
    public Collection<SocketAddress> resolveOnce( SocketAddress initialAddress )
    {
        try
        {
            Set<SocketAddress> addresses = srvRecordResolver
                    .resolveSrvRecord( initialAddress.getHostname() )
                    .map( srvRecord -> new SocketAddress( srvRecord.host, srvRecord.port ) )
                    .collect( Collectors.toSet() );

            userLog.info( "Resolved initial host '%s' to %s", initialAddress, addresses );

            if ( addresses.isEmpty() )
            {
                log.error( "Failed to resolve srv records for '%s'", initialAddress.getHostname() );
            }

            return addresses;
        }
        catch ( NamingException e )
        {
            log.error( String.format( "Failed to resolve srv records for '%s'", initialAddress.getHostname() ), e );
            return Collections.emptySet();
        }
    }
}
