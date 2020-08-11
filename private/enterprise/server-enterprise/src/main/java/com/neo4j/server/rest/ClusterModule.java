/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import com.neo4j.server.rest.causalclustering.ClusteringDatabaseService;
import com.neo4j.server.rest.causalclustering.ClusteringDbmsService;

import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.web.WebServer;

public class ClusterModule implements ServerModule
{
    private final WebServer server;

    private final String dbMountPoint;
    private final List<Class<?>> dbJaxRsClasses;

    private final String dbmsMountPoint;
    private final List<Class<?>> dbmsJaxRsClasses;

    public ClusterModule( WebServer server, Config config )
    {
        this.server = server;
        this.dbMountPoint = config.get( ServerSettings.db_api_path ).toString();
        this.dbJaxRsClasses = List.of( ClusteringDatabaseService.class );
        this.dbmsMountPoint = ServerSettings.DBMS_MOUNT_POINT;
        this.dbmsJaxRsClasses = List.of( ClusteringDbmsService.class );
    }

    @Override
    public void start()
    {
        server.addJAXRSClasses( dbJaxRsClasses, dbMountPoint, null );
        server.addJAXRSClasses( dbmsJaxRsClasses, dbmsMountPoint, null );
    }

    @Override
    public void stop()
    {
        server.removeJAXRSClasses( dbJaxRsClasses, dbMountPoint );
        server.removeJAXRSClasses( dbmsJaxRsClasses, dbmsMountPoint );
    }
}
