/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helpers;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.neo4j.test.extension.StatefulFieldExtension;

public class BuffersExtension extends StatefulFieldExtension<Buffers> implements AfterEachCallback, BeforeEachCallback
{
    private static final String BUFFERS = "buffers";
    private static final ExtensionContext.Namespace BUFFERS_NAMESPACE = ExtensionContext.Namespace.create( BUFFERS );

    @Override
    protected String getFieldKey()
    {
        return BUFFERS;
    }

    @Override
    protected Class<Buffers> getFieldType()
    {
        return Buffers.class;
    }

    @Override
    protected Buffers createField( ExtensionContext extensionContext )
    {
        return new Buffers();
    }

    @Override
    protected ExtensionContext.Namespace getNameSpace()
    {
        return BUFFERS_NAMESPACE;
    }

    @Override
    public void beforeEach( ExtensionContext context ) throws Exception
    {
        var buffers = getStoredValue( context );
        if ( buffers != null )
        {
            buffers.before();
        }
        super.afterAll( context );
    }

    @Override
    public void afterEach( ExtensionContext context ) throws Exception
    {
        var buffers = getStoredValue( context );
        if ( buffers != null )
        {
            buffers.after();
        }
        super.afterAll( context );
    }
}
