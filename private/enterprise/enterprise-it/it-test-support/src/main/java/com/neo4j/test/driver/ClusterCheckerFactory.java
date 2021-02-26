/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.driver;

import com.neo4j.causalclustering.common.Cluster;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ClusterCheckerFactory extends CloseableFactory implements Closeable
{
    private final DriverFactory driverFactory;

    public ClusterCheckerFactory( DriverFactory driverFactory )
    {
        this.driverFactory = driverFactory;
    }

    public ClusterChecker clusterChecker( Collection<URI> boltURIs ) throws IOException
    {
        return addCloseable( ClusterChecker.fromBoltURIs( boltURIs, this.driverFactory::graphDatabaseDriver ) );
    }

    public ClusterChecker clusterChecker( Collection<URI> boltURIs, DriverFactory.InstanceConfig driverConfig ) throws IOException
    {
        return addCloseable( ClusterChecker.fromBoltURIs( boltURIs, uri -> this.driverFactory.graphDatabaseDriver( uri, driverConfig ) ) );
    }

    public ClusterChecker clusterChecker( Cluster cluster ) throws IOException
    {
        List<URI> coreUris = cluster.coreMembers()
                                    .stream()
                                    .filter( c -> !c.isShutdown() )
                                    .map( c -> URI.create( c.directURI() ) )
                                    .collect( Collectors.toList() );
        return clusterChecker( coreUris );
    }

    public ClusterChecker clusterChecker( Cluster cluster, DriverFactory.InstanceConfig driverConfig ) throws IOException
    {
        List<URI> coreUris = cluster.coreMembers()
                                    .stream()
                                    .filter( c -> !c.isShutdown() )
                                    .map( c -> URI.create( c.directURI() ) )
                                    .collect( Collectors.toList() );
        return clusterChecker( coreUris, driverConfig );
    }
}
