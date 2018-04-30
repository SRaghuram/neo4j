/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.ssl.SslPolicy;

public interface SslDiscoveryServiceFactory extends DiscoveryServiceFactory
{
    void setSslPolicy( SslPolicy sslPolicy );
}
