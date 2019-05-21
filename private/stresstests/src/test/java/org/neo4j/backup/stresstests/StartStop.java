/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.stresstests;

import com.neo4j.causalclustering.stresstests.Control;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helper.Workload;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class StartStop extends Workload
{
    private final AtomicReference<GraphDatabaseService> dbRef;
    private final DatabaseManagementService managementService;

    StartStop( Control control, AtomicReference<GraphDatabaseService> dbRef,
            DatabaseManagementService managementService )
    {
        super( control );
        this.dbRef = dbRef;
        this.managementService = managementService;
    }

    @Override
    protected void doWork() throws Exception
    {
        TimeUnit.SECONDS.sleep( 10 ); // sleep between runs
        GraphDatabaseService db = dbRef.get();
        managementService.shutdownDatabase( DEFAULT_DATABASE_NAME );
        TimeUnit.SECONDS.sleep( 2 ); // sleep a bit while db is shutdown to let backup fail
        boolean replaced = dbRef.compareAndSet( db, restartDatabase() );
        assertTrue( replaced );
    }

    private GraphDatabaseService restartDatabase()
    {
        managementService.startDatabase( DEFAULT_DATABASE_NAME );
        return managementService.database( DEFAULT_DATABASE_NAME );
    }
}
