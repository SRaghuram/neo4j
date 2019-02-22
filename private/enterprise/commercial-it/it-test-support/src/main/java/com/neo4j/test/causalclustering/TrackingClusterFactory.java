/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.causalclustering;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.DefaultCluster;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.helper.ErrorHandler;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.test.rule.TestDirectory;

public class TrackingClusterFactory implements ClusterFactory
{
    private TestDirectory testDirectory;
    private final TestInstance.Lifecycle lifecycle;
    private final Collection<Cluster> clusters = new CopyOnWriteArrayList<>();
    private final AtomicInteger idCounter = new AtomicInteger();
    private String initialFailure;

    TrackingClusterFactory( TestDirectory testDirectory, TestInstance.Lifecycle lifecycle )
    {
        this.testDirectory = testDirectory;
        this.lifecycle = lifecycle;
    }

    public TestInstance.Lifecycle getLifecycle()
    {
        return lifecycle;
    }

    @Override
    public Cluster<DiscoveryServiceFactory> createCluster( ClusterConfig clusterConfig )
    {
        File directory = testDirectory.directory( generateId() );
        DefaultCluster cluster = ClusterConfig.createCluster( directory, clusterConfig );
        clusters.add( cluster );
        return cluster;
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

    void prepareDirectory( ExtensionContext context ) throws IOException
    {
        testDirectory.prepareDirectory( context.getRequiredTestClass(), context.getRequiredTestMethod().getName() );
    }

    void completeDirectory() throws IOException
    {
        testDirectory.complete( initialFailure == null );
    }

    boolean disallowContinue()
    {
        return initialFailure != null && lifecycle == TestInstance.Lifecycle.PER_CLASS;
    }

    String getInitialFailure()
    {
        return initialFailure;
    }

    void setFailed( String name )
    {
        if ( initialFailure == null )
        {
            initialFailure = name;
        }
    }
}
