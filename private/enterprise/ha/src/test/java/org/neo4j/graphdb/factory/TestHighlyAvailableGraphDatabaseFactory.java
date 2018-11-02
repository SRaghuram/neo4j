/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.graphdb.factory;

import java.util.Collections;
import java.util.function.Predicate;

import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.ha.ClusterManager;

public class TestHighlyAvailableGraphDatabaseFactory extends HighlyAvailableGraphDatabaseFactory
{
    @Override
    protected void configure( GraphDatabaseBuilder builder )
    {
        super.configure( builder );
        builder.setConfig( ClusterManager.CONFIG_FOR_SINGLE_JVM_CLUSTER );
        builder.setConfig( GraphDatabaseSettings.store_internal_log_level, "DEBUG" );
    }

    public TestHighlyAvailableGraphDatabaseFactory addKernelExtensions( Iterable<KernelExtensionFactory<?>> newKernelExtensions )
    {
        getCurrentState().addKernelExtensions( newKernelExtensions );
        return this;
    }

    public TestHighlyAvailableGraphDatabaseFactory addKernelExtension( KernelExtensionFactory<?> newKernelExtension )
    {
        return addKernelExtensions( Collections.singletonList( newKernelExtension ) );
    }

    public TestHighlyAvailableGraphDatabaseFactory setKernelExtensions( Iterable<KernelExtensionFactory<?>> newKernelExtensions )
    {
        getCurrentState().setKernelExtensions( newKernelExtensions );
        return this;
    }

    public TestHighlyAvailableGraphDatabaseFactory removeKernelExtensions( Predicate<KernelExtensionFactory<?>> toRemove )
    {
        getCurrentState().removeKernelExtensions( toRemove );
        return this;
    }
}
