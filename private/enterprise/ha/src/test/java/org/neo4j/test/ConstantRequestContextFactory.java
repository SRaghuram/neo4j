/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.test;

import org.neo4j.com.RequestContext;
import org.neo4j.kernel.ha.com.RequestContextFactory;

public class ConstantRequestContextFactory extends RequestContextFactory
{
    private final RequestContext requestContext;

    public ConstantRequestContextFactory( RequestContext requestContext )
    {
        super( 0, null );
        this.requestContext = requestContext;
    }

    @Override
    public RequestContext newRequestContext( long epoch, int machineId, int eventIdentifier )
    {
        return requestContext;
    }
}
