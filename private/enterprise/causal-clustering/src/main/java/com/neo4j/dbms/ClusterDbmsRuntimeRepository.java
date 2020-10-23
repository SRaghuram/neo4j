/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventListener;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.dbms.database.DbmsRuntimeVersion;

/**
 * A version of {@link} DbmsRuntimeRepository for cluster editions.
 */
public class ClusterDbmsRuntimeRepository extends DbmsRuntimeRepository implements ReplicatedDatabaseEventListener
{

    public ClusterDbmsRuntimeRepository( DatabaseManager<?> databaseManager )
    {
        super( databaseManager );
    }

    @Override
    protected DbmsRuntimeVersion getFallbackVersion()
    {
        // New components are not currently initialised in cluster deployment when new binaries are booted on top of an existing database.
        // This is a known shortcoming of the lifecycle of System graph components.
        // As the result of above, if the execution path makes it here, it means new binaries are booted on top of an existing database.
        return PREVIOUS_VERSION;
    }

    @Override
    public void transactionCommitted( long txId )
    {
        maybeUpdateStateFromSystemDatabase();
    }

    @Override
    public void storeReplaced( long txId )
    {
        maybeUpdateStateFromSystemDatabase();
    }

    private void maybeUpdateStateFromSystemDatabase()
    {
        // no check is needed if we are at the latest version, because downgrade is not supported
        if ( getVersion() != LATEST_VERSION )
        {
            fetchStateFromSystemDatabase();
        }
    }
}
