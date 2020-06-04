/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.routing;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.neo4j.bolt.v41.messaging.RoutingContext;
import org.neo4j.fabric.bootstrap.TestOverrides;
import org.neo4j.test.extension.StatefulFieldExtension;

import static org.mockito.Mockito.mock;

public class MockedRoutingContextExtension extends StatefulFieldExtension<RoutingContext> implements BeforeAllCallback, AfterAllCallback
{
    private static final String ROUTING_CONTEXT = "routing-context";
    private static final ExtensionContext.Namespace ROUTING_CONTEXT_NAMESPACE = ExtensionContext.Namespace.create( ROUTING_CONTEXT );

    @Override
    protected String getFieldKey()
    {
        return ROUTING_CONTEXT;
    }

    @Override
    protected Class<RoutingContext> getFieldType()
    {
        return RoutingContext.class;
    }

    @Override
    protected RoutingContext createField( ExtensionContext extensionContext )
    {
        createMockIfNotPresent();
        return TestOverrides.ROUTING_CONTEXT.get();
    }

    @Override
    protected ExtensionContext.Namespace getNameSpace()
    {
        return ROUTING_CONTEXT_NAMESPACE;
    }

    @Override
    public void beforeAll( ExtensionContext extensionContext )
    {
        createMockIfNotPresent();
    }

    @Override
    public void afterAll( ExtensionContext extensionContext )
    {
        TestOverrides.ROUTING_CONTEXT.remove();
    }

    private void createMockIfNotPresent()
    {
        if ( TestOverrides.ROUTING_CONTEXT.get() == null )
        {
            TestOverrides.ROUTING_CONTEXT.set( mock( RoutingContext.class ) );
        }
    }
}
