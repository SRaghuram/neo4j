/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

public class EnterpriseDiscoveryServiceFactorySelector extends DiscoveryServiceFactorySelector<DiscoveryServiceFactory>
{
    @Override
    protected DiscoveryServiceFactory select( DiscoveryImplementation middleware )
    {
        switch ( middleware )
        {
        case HAZELCAST: return new HazelcastDiscoveryServiceFactory();
        case AKKA: throw new UnsupportedOperationException( "Akka based discovery is Commercial release only" );
        default: throw new IllegalArgumentException( "Should have matched a discovery service factory to " + middleware );
        }
    }
}
