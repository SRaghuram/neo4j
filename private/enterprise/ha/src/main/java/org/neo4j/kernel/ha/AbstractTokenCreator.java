/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import org.neo4j.com.ComException;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.core.TokenCreator;

public abstract class AbstractTokenCreator implements TokenCreator
{
    private final Master master;
    private final RequestContextFactory requestContextFactory;

    protected AbstractTokenCreator( Master master, RequestContextFactory requestContextFactory )
    {
        this.master = master;
        this.requestContextFactory = requestContextFactory;
    }

    @Override
    public final int createToken( String name )
    {
        try ( Response<Integer> response = create( master, requestContextFactory.newRequestContext(), name ) )
        {
            return response.response();
        }
        catch ( ComException e )
        {
            throw new TransientTransactionFailureException(
                    "Cannot create identifier for token '" + name + "' on the master " + master + ". " +
                    "The master is either down, or we have network connectivity problems", e );
        }
    }

    protected abstract Response<Integer> create( Master master, RequestContext context, String name );
}
