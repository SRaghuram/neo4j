/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.causalclustering;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.StatefulFieldExtension;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;

/**
 * Extension for cluster ITs. Allows the user to {@link Inject} a {@link ClusterFactory} into the test class. Clusters created will have the same lifecycle
 * as the {@link TestInstance.Lifecycle} of the root class.
 */
public class ClusterFactoryExtension extends StatefulFieldExtension<ClusterFactory> implements AfterEachCallback, ExecutionCondition
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
            shutdownCluster( clusterFactory );
        }
    }

    private static void shutdownCluster( TrackingClusterFactory clusterFactory )
    {
        clusterFactory.shutdownAll();
    }

    /**
     * If the context created the {@link ClusterFactory} and has PER_METHOD lifecycle then we shutdown all clusters.
     */
    @Override
    public void afterEach( ExtensionContext context )
    {
        TrackingClusterFactory clusterFactory = (TrackingClusterFactory) getStoredValue( context );
        context.getExecutionException().ifPresent( e -> clusterFactory.setFailed( context.getDisplayName() ) );
        if ( clusterFactory.getLifecycle() == TestInstance.Lifecycle.PER_METHOD )
        {
            shutdownCluster( clusterFactory );
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
        return extensionContext
                .getStore( TestDirectorySupportExtension.TEST_DIRECTORY_NAMESPACE )
                .get( TestDirectorySupportExtension.TEST_DIRECTORY, TestDirectory.class );
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition( ExtensionContext context )
    {
        /*
        If in PER_CLASS mode and there has been a failed test then we ignore the remaining tests. This because they share the same cluster(s) throughout the
        lifecycle and could therefore cause succumbing methods to fail. It avoids having to figure out what test was the root cause of failure and possibly
        wasting time investigating a failed test that was just failing due to a prior test method failing.

        NOTE! This does not work with dynamic tests since they do not support lifecycle callbacks!
         */
        TrackingClusterFactory clusterFactory = (TrackingClusterFactory) getStoredValue( context );
        if ( clusterFactory != null && clusterFactory.disallowContinue() )
        {
            return ConditionEvaluationResult.disabled(
                    format( "A test method failed prior to this. Since they share cluster(s) this test is ignored. The initial failing test method was: '%s'",
                            clusterFactory.getInitialFailure() ) );
        }
        else
        {
            return ConditionEvaluationResult.enabled( "" );
        }
    }
}
