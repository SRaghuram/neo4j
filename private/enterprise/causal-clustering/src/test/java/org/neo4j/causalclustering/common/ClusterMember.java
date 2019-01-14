/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.common;

import java.io.File;

import org.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;

public interface ClusterMember<T extends GraphDatabaseAPI>
{
    void start();

    void shutdown();

    boolean hasPanicked();

    boolean isShutdown();

    T database();

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

    File databaseDirectory();

    File homeDir();

    int serverId();

    default void updateConfig( Setting<?> setting, String value )
    {
        config().augment( setting, value );
    }
}
