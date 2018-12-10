/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.stresstests;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.neo4j.causalclustering.stresstests.Control;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helper.Workload;

import static org.junit.Assert.assertTrue;

class StartStop extends Workload
{
    private final AtomicReference<GraphDatabaseService> dbRef;
    private final Factory<GraphDatabaseService> factory;

    StartStop( Control control, Factory<GraphDatabaseService> factory, AtomicReference<GraphDatabaseService> dbRef )
    {
        super( control );
        this.factory = factory;
        this.dbRef = dbRef;
    }

    @Override
    protected void doWork() throws Exception
    {
        TimeUnit.SECONDS.sleep( 10 ); // sleep between runs
        GraphDatabaseService db = dbRef.get();
        db.shutdown();
        TimeUnit.SECONDS.sleep( 2 ); // sleep a bit while db is shutdown to let backup fail
        boolean replaced = dbRef.compareAndSet( db, factory.newInstance() );
        assertTrue( replaced );
    }
}
