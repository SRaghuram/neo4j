/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.test.causalclustering;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;

import org.neo4j.causalclustering.helper.ErrorHandler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.StatefullFieldExtension;
import org.neo4j.test.rule.TestDirectory;

/**
 * Extension for cluster ITs. Allows the user to {@link Inject} a {@link ClusterFactory} into the test class. Clusters created will have the same lifecycle
 * as the {@link TestInstance.Lifecycle} of the root class.
 */
class ClusterFactoryExtension extends StatefullFieldExtension<ClusterFactory> implements AfterEachCallback
{
    private static final String CLUSTER = "cluster";
    private static final ExtensionContext.Namespace CLUSTER_NAMESPACE = ExtensionContext.Namespace.create( CLUSTER );

    /**
     * Shuts down all clusters only if the extension context was {@link Inject} by this test class. The reason being that shutdown could still be called by
     * a {@link Nested} class where the extension comes from the parent class. In such a case we should not shut down.
     */
    @Override
    public void afterAll( ExtensionContext context )
    {
        TrackingClusterFactory clusterFactory = (TrackingClusterFactory) removeStoredValue( context );
        if ( clusterFactory != null )
        {
            try ( ErrorHandler errorHandler = new ErrorHandler( "Shutting down cluster contexts" ) )
            {
                errorHandler.execute( clusterFactory::shutdownAll );
                errorHandler.execute( () -> clusterFactory.testDirectory().complete( !context.getExecutionException().isPresent() ) );
            }
        }
    }

    /**
     * If the context created the {@link ClusterFactory} and has PER_METHOD lifecycle then we shutdown all clusters.
     */
    @Override
    public void afterEach( ExtensionContext context )
    {
        TrackingClusterFactory clusterFactory = (TrackingClusterFactory) getStoredValue( context );
        if ( clusterFactory.getLifecycle() == TestInstance.Lifecycle.PER_METHOD )
        {
            clusterFactory.shutdownAll();
        }
    }

    @Override
    protected String getFieldKey()
    {
        return CLUSTER;
    }

    @Override
    protected Class<ClusterFactory> getFieldType()
    {
        return ClusterFactory.class;
    }

    @Override
    protected ClusterFactory createField( ExtensionContext extensionContext )
    {
        TestDirectory testDirectory = getTestDirectory( extensionContext );
        return new TrackingClusterFactory( testDirectory, extensionContext.getTestInstanceLifecycle().orElse( TestInstance.Lifecycle.PER_CLASS ) );
    }

    @Override
    protected ExtensionContext.Namespace getNameSpace()
    {
        return CLUSTER_NAMESPACE;
    }

    private static TestDirectory getTestDirectory( ExtensionContext extensionContext )
    {
        TestDirectory testDirectory = TestDirectory.testDirectory();
        try
        {
            testDirectory.prepareDirectory( extensionContext.getRequiredTestClass(), "class" );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return testDirectory;
    }
}
