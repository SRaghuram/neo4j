/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import org.neo4j.ssl.SslPolicy;

public class SslSharedDiscoveryServiceFactory extends SharedDiscoveryServiceFactory implements SslDiscoveryServiceFactory
{
    @Override
    public void setSslPolicy( SslPolicy sslPolicy )
    {
        // meh
    }
}
