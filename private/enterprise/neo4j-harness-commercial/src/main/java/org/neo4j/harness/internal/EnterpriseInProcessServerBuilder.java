/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.harness.internal;

import java.io.File;

import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory.Dependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.AbstractNeoServer;
import org.neo4j.server.database.EnterpriseGraphFactory;
import org.neo4j.server.database.GraphFactory;
import org.neo4j.server.enterprise.OpenEnterpriseNeoServer;

public class EnterpriseInProcessServerBuilder extends AbstractInProcessServerBuilder
{
    public EnterpriseInProcessServerBuilder()
    {
        this( new File( System.getProperty( "java.io.tmpdir" ) ) );
    }

    public EnterpriseInProcessServerBuilder( File workingDir )
    {
        super( workingDir );
    }

    public EnterpriseInProcessServerBuilder( File workingDir, String dataSubDir )
    {
        super( workingDir, dataSubDir );
    }

    @Override
    protected GraphFactory createGraphFactory( Config config )
    {
        return new EnterpriseGraphFactory();
    }

    @Override
    protected AbstractNeoServer createNeoServer( GraphFactory graphFactory, Config config, Dependencies dependencies )
    {
        return new OpenEnterpriseNeoServer( config, graphFactory, dependencies );
    }
}
