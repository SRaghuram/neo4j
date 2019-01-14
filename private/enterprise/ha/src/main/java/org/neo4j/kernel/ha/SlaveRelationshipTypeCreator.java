/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;

public class SlaveRelationshipTypeCreator extends AbstractTokenCreator
{
    public SlaveRelationshipTypeCreator( Master master, RequestContextFactory requestContextFactory )
    {
        super( master, requestContextFactory );
    }

    @Override
    protected Response<Integer> create( Master master, RequestContext context, String name )
    {
        return master.createRelationshipType( context, name );
    }
}
