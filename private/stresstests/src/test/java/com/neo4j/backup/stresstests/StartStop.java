/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.stresstests;

import com.neo4j.causalclustering.stresstests.Control;
import com.neo4j.helper.Workload;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;

import static org.junit.jupiter.api.Assertions.assertTrue;

class StartStop extends Workload
{
    private final AtomicReference<GraphDatabaseService> dbRef;
    private final DatabaseManagementService managementService;
    private final String databaseName;

    StartStop( Control control, AtomicReference<GraphDatabaseService> dbRef,
            DatabaseManagementService managementService, String databaseName )
    {
        super( control );
        this.dbRef = dbRef;
        this.managementService = managementService;
        this.databaseName = databaseName;
    }

    @Override
    protected void doWork() throws Exception
    {
        TimeUnit.SECONDS.sleep( 10 ); // sleep between runs
        GraphDatabaseService db = dbRef.get();
        managementService.shutdownDatabase( databaseName );
        TimeUnit.SECONDS.sleep( 2 ); // sleep a bit while db is shutdown to let backup fail
        boolean replaced = dbRef.compareAndSet( db, restartDatabase() );
        assertTrue( replaced );
    }

    private GraphDatabaseService restartDatabase()
    {
        managementService.startDatabase( databaseName );
        return managementService.database( databaseName );
    }
}
