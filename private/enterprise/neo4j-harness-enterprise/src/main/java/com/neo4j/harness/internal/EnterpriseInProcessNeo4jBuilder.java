/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness.internal;

import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.server.enterprise.EnterpriseManagementServiceFactory;
import org.apache.commons.lang3.SystemUtils;

import java.io.File;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.harness.internal.AbstractInProcessNeo4jBuilder;

public class EnterpriseInProcessNeo4jBuilder extends AbstractInProcessNeo4jBuilder
{
    public EnterpriseInProcessNeo4jBuilder()
    {
        this( SystemUtils.getJavaIoTmpDir() );
    }

    public EnterpriseInProcessNeo4jBuilder( File workingDir )
    {
        withWorkingDir( workingDir );
        withConfig( OnlineBackupSettings.online_backup_listen_address, new SocketAddress( "localhost", 0 ) );
    }

    public EnterpriseInProcessNeo4jBuilder( File workingDir, String dataSubDir )
    {
        super( workingDir, dataSubDir );
    }

    @Override
    protected DatabaseManagementService createNeo( Config config, ExternalDependencies dependencies )
    {
        return EnterpriseManagementServiceFactory.createManagementService( config, dependencies );
    }
}
