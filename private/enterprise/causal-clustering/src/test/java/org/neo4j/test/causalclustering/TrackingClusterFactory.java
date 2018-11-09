/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.test.causalclustering;

import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.common.EnterpriseCluster;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.helper.ErrorHandler;
import org.neo4j.test.rule.TestDirectory;

public class TrackingClusterFactory implements ClusterFactory
{
    private TestDirectory testDirectory;
    private final Class<?> injectedClass;
    private final TestInstance.Lifecycle lifecycle;
    private final Collection<Cluster> clusters = new CopyOnWriteArrayList<>();
    private static final AtomicInteger idCounter = new AtomicInteger();

    TrackingClusterFactory( TestDirectory testDirectory, Class<?> injectedClass, TestInstance.Lifecycle lifecycle )
    {
        this.testDirectory = testDirectory;
        this.injectedClass = injectedClass;
        this.lifecycle = lifecycle;
    }

    @Override
    public Cluster<DiscoveryServiceFactory> createCluster( ClusterConfig clusterConfig )
    {
        File directory = testDirectory.directory( generateId() );
        EnterpriseCluster cluster = ClusterConfig.createCluster( directory, clusterConfig );
        clusters.add( cluster );
        return cluster;
    }

    boolean isRootClass( Class<?> clazz )
    {
        return clazz.equals( injectedClass );
    }

    public TestInstance.Lifecycle getLifecycle()
    {
        return lifecycle;
    }

    void shutdownAll()
    {
        shutdown( clusters.iterator() );
    }

    private void shutdown( Iterator<Cluster> iterator )
    {
        try ( ErrorHandler errorHandler = new ErrorHandler( "Shutting down cluster contexts" ) )
        {
            ArrayList<Cluster> toRemove = new ArrayList<>();
            iterator.forEachRemaining( cluster ->
            {
                errorHandler.execute( cluster::shutdown );
                toRemove.add( cluster );
            } );
            clusters.removeAll( toRemove );
        }
    }

    int activeClusters()
    {
        return clusters.size();
    }

    private String generateId()
    {
        return "cluster-" + idCounter.getAndIncrement();
    }

    public TestDirectory testDirectory()
    {
        return testDirectory;
    }
}
