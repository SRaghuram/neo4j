/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class MapDomainNameResolver implements DomainNameResolver
{
    private final Map<String /*hostname*/,InetAddress[] /*response*/> domainNameMapping;

    public MapDomainNameResolver( Map<String,InetAddress[]> mapping )
    {
        domainNameMapping = mapping;
    }

    @Override
    public InetAddress[] resolveDomainName( String hostname ) throws UnknownHostException
    {
        return Optional.ofNullable( domainNameMapping.get( hostname ) ).orElse( new InetAddress[0] );
    }

    public void setHostnameAddresses( String hostname, Collection<String> addresses )
    {
        InetAddress[] processedAddresses = new InetAddress[addresses.size()];
        addresses.stream().map( MapDomainNameResolver::inetAddress ).collect( Collectors.toList() )
                .toArray( processedAddresses );
        domainNameMapping.put( hostname, processedAddresses );
    }

    private static InetAddress inetAddress( String address )
    {
        try
        {
            return InetAddress.getByName( address );
        }
        catch ( java.net.UnknownHostException e )
        {
            throw new UnknownHostException( e );
        }
    }
}
