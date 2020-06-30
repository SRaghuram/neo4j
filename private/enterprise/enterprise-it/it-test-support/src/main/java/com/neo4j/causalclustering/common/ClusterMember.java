/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.discovery.ConnectorAddresses;
import com.neo4j.causalclustering.identity.MemberId;

import java.nio.file.Path;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.monitoring.Monitors;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;

public interface ClusterMember
{
    MemberId id();

    void start();

    void shutdown();

    boolean isShutdown();

    DatabaseManagementService managementService();

    default GraphDatabaseFacade defaultDatabase()
    {
        return database( config().get( default_database ) );
    }

    default GraphDatabaseFacade systemDatabase()
    {
        return database( SYSTEM_DATABASE_NAME );
    }

    default GraphDatabaseFacade database( String databaseName )
    {
        return (GraphDatabaseFacade) managementService().database( databaseName );
    }

    ConnectorAddresses clientConnectorAddresses();

    <T> T settingValue( Setting<T> setting );

    Config config();

    /**
     * {@link Cluster} will use this {@link ThreadGroup} for the threads that start, and shut down, this cluster member.
     * This way, the group will be transitively inherited by all the threads that are in turn started by the member
     * during its start up and shut down processes.
     * <p>
     * This helps with debugging, because it makes it immediately visible (in the debugger) which cluster member any
     * given thread belongs to.
     *
     * @return The intended parent thread group for this cluster member.
     */
    ThreadGroup threadGroup();

    Monitors monitors();

    DatabaseLayout databaseLayout();

    Path homePath();

    Neo4jLayout neo4jLayout();

    int serverId();

    String boltAdvertisedAddress();

    String intraClusterBoltAdvertisedAddress();

    default <T> void updateConfig( Setting<T> setting, T value )
    {
        config().set( setting, value );
    }

    default <T> T resolveDependency( String databaseName, Class<T> type )
    {
        return database( databaseName ).getDependencyResolver().resolveDependency( type );
    }
}
