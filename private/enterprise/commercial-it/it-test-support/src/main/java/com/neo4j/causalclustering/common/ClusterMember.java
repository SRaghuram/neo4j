/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import com.neo4j.causalclustering.identity.MemberId;

import java.io.File;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.monitoring.Monitors;

public interface ClusterMember
{
    MemberId id();

    void start();

    void shutdown();

    boolean hasPanicked();

    boolean isShutdown();

    DatabaseManagementService managementService();

    GraphDatabaseFacade defaultDatabase();

    GraphDatabaseFacade systemDatabase();

    GraphDatabaseFacade database( String databaseName );

    ClientConnectorAddresses clientConnectorAddresses();

    String settingValue( String settingName );

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

    File homeDir();

    int serverId();

    String boltAdvertisedAddress();

    default void updateConfig( Setting<?> setting, String value )
    {
        config().augment( setting, value );
    }
}
