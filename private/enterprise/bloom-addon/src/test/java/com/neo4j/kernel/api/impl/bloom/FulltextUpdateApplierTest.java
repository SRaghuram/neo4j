/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;
import java.util.concurrent.ExecutionException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.scheduler.JobScheduler;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class FulltextUpdateApplierTest
{
    private LifeSupport life;
    private FulltextUpdateApplier applier;
    private AvailabilityGuard availabilityGuard;
    private JobScheduler scheduler;
    private Log log;

    @Before
    public void setUp()
    {
        life = new LifeSupport();
        log = NullLog.getInstance();
        availabilityGuard = new AvailabilityGuard( Clock.systemUTC(), log );
        scheduler = life.add( new CentralJobScheduler() );
        life.start();
    }

    private void startApplier()
    {
        applier = life.add( new FulltextUpdateApplier( log, availabilityGuard, scheduler ) );
    }

    @After
    public void tearDown()
    {
        life.shutdown();
    }

    @Test
    public void exceptionsDuringIndexUpdateMustPropagateToTheCaller() throws Exception
    {
        startApplier();
        AsyncFulltextIndexOperation op = applier.updatePropertyData( null, null );

        try
        {
            op.awaitCompletion();
            fail( "awaitCompletion should have thrown" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), is( instanceOf( NullPointerException.class ) ) );
        }
    }

    @Test
    public void exceptionsDuringNodePopulationMustBeLoggedAndMarkTheIndexAsFailed() throws Exception
    {
        startApplier();
        LuceneFulltext index = new StubLuceneFulltext();
        GraphDatabaseService db = new StubGraphDatabaseService();
        WritableFulltext writableFulltext = new WritableFulltext( index );
        AsyncFulltextIndexOperation op = applier.populateNodes( writableFulltext, db );

        try
        {
            op.awaitCompletion();
            fail( "awaitCompletion should have thrown" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), is( instanceOf( NullPointerException.class ) ) );
        }
        assertThat( index.getState(), is( InternalIndexState.FAILED ) );
    }

    @Test
    public void exceptionsDuringRelationshipPopulationMustBeLoggedAndMarkTheIndexAsFailed() throws Exception
    {
        startApplier();
        LuceneFulltext index = new StubLuceneFulltext();
        GraphDatabaseService db = new StubGraphDatabaseService();
        WritableFulltext writableFulltext = new WritableFulltext( index );
        AsyncFulltextIndexOperation op = applier.populateRelationships( writableFulltext, db );

        try
        {
            op.awaitCompletion();
            fail( "awaitCompletion should have thrown" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), is( instanceOf( NullPointerException.class ) ) );
        }
        assertThat( index.getState(), is( InternalIndexState.FAILED ) );
    }
}
