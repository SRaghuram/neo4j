/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import java.net.InetAddress;

public class DomainNameResolverImpl implements DomainNameResolver
{
    @Override
    public InetAddress[] resolveDomainName( String hostname ) throws UnknownHostException
    {
        try
        {
            return InetAddress.getAllByName( hostname );
        }
        catch ( java.net.UnknownHostException e )
        {
            throw new UnknownHostException( e );
        }
    }
}
