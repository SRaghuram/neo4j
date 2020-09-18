/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.clusterdockertests;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.junit.platform.commons.support.AnnotationSupport;

import java.lang.reflect.Field;

import org.neo4j.junit.jupiter.causal_cluster.CausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jCluster;

public class DumpDockerLogs implements TestWatcher
{
    @Override
    public void testFailed( ExtensionContext context, Throwable cause )
    {
        printContainerLogs( context );
    }

    private void printContainerLogs( ExtensionContext context )
    {
        Object testInstance = context.getRequiredTestInstance();
        Field clusterField = AnnotationSupport.findAnnotatedFields( context.getRequiredTestClass(), CausalCluster.class ).stream()
                                              .filter( field -> field.getType() == Neo4jCluster.class ).findFirst().get();
        Neo4jCluster cluster;
        try
        {
            clusterField.setAccessible( true );
            cluster = (Neo4jCluster) clusterField.get( testInstance );
        }
        catch ( IllegalAccessException e )
        {
            throw new RuntimeException( e );
        }
        cluster.getAllServers().forEach( s -> System.out.println(s.getContainerLogsSinceStart()) );
        cluster.getAllServers().forEach( s -> System.out.println(s.getDebugLog()) );
    }

    @Override
    public void testAborted( ExtensionContext context, Throwable cause )
    {
        printContainerLogs( context );
    }
}
