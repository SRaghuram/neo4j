/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.kernel.configuration.Config;

public abstract class DiscoveryServiceFactorySelector<T extends DiscoveryServiceFactory>
{
    public static DiscoveryImplementation DEFAULT = DiscoveryImplementation.HAZELCAST;

    public T select( Config config )
    {
        DiscoveryImplementation middleware = config.get( CausalClusteringSettings.discovery_implementation );
        return select( middleware );
    }

    protected abstract T select( DiscoveryImplementation middleware );

    public enum DiscoveryImplementation
    {
        HAZELCAST,
        AKKA
    }
}
