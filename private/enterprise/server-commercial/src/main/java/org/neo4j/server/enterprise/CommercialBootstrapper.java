/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.enterprise;

import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.NeoServer;

public class CommercialBootstrapper extends EnterpriseBootstrapper
{
    @Override
    protected NeoServer createNeoServer( Config configurator, GraphDatabaseDependencies dependencies,
            LogProvider userLogProvider )
    {
        return new CommercialNeoServer( configurator, dependencies, userLogProvider );
    }
}
