/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness.internal;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.server.database.CommercialGraphFactory;
import com.neo4j.server.enterprise.CommercialNeoServer;
import org.apache.commons.lang3.SystemUtils;

import java.io.File;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.harness.internal.AbstractInProcessNeo4jBuilder;
import org.neo4j.server.AbstractNeoServer;
import org.neo4j.server.database.GraphFactory;

public class CommercialInProcessNeo4jBuilder extends AbstractInProcessNeo4jBuilder
{
    public CommercialInProcessNeo4jBuilder()
    {
        this( SystemUtils.getJavaIoTmpDir() );
    }

    public CommercialInProcessNeo4jBuilder( File workingDir )
    {
        withWorkingDir( workingDir );
        withConfig( OnlineBackupSettings.online_backup_listen_address, "localhost:0" );
    }

    public CommercialInProcessNeo4jBuilder( File workingDir, String dataSubDir )
    {
        super( workingDir, dataSubDir );
    }

    @Override
    protected GraphFactory createGraphFactory( Config config )
    {
        return new CommercialGraphFactory();
    }

    @Override
    protected AbstractNeoServer createNeoServer( GraphFactory graphFactory, Config config, ExternalDependencies dependencies )
    {
        return new CommercialNeoServer( config, graphFactory, dependencies );
    }

}
