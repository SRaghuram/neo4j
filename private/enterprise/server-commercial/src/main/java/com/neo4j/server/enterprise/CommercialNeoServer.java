/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.server.database.CommercialGraphFactory;

import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory.Dependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.database.GraphFactory;
import org.neo4j.server.enterprise.OpenEnterpriseNeoServer;

public class CommercialNeoServer extends OpenEnterpriseNeoServer
{
    public CommercialNeoServer( Config config, Dependencies dependencies )
    {
        super( config, new CommercialGraphFactory(), dependencies );
    }

    public CommercialNeoServer( Config config, GraphFactory graphFactory, Dependencies dependencies )
    {
        super( config, graphFactory, dependencies );
    }
}
