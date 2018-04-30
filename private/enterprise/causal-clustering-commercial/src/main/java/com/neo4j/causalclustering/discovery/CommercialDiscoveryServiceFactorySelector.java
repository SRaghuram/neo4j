/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.akka.CommercialAkkaDiscoveryServiceFactory;

import org.neo4j.causalclustering.discovery.DiscoveryServiceFactorySelector;

public class CommercialDiscoveryServiceFactorySelector extends DiscoveryServiceFactorySelector<SslDiscoveryServiceFactory>
{
    @Override
    protected SslDiscoveryServiceFactory select( DiscoveryMiddleware middleware )
    {
        switch ( middleware )
        {
        case hazelcast: return new SslHazelcastDiscoveryServiceFactory();
        case akka: return new CommercialAkkaDiscoveryServiceFactory();
        default: throw new IllegalArgumentException( "Should have matched a discovery service factory to " + middleware );
        }
    }
}
