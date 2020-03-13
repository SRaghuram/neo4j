/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class ReconcilerExecutorsTest
{
    private final JobScheduler jobScheduler = JobSchedulerFactory.createScheduler();
    private Semaphore semaphore;

    @BeforeEach
    void setup() throws Exception
    {
        jobScheduler.start();
        semaphore = new Semaphore( 2 ); // 2 threads for priority and non-priority
        semaphore.acquire( 2 );
    }

    @AfterEach
    void cleanup() throws Exception
    {
        semaphore.release( 2 );
        jobScheduler.shutdown();
    }

    @Test
    void shouldUsePriorityExecutorForSimpleSystemDbRequests()
    {
        // given
        var config = Config.defaults( GraphDatabaseSettings.reconciler_maximum_parallelism, 2 );
        var executors = new ReconcilerExecutors( jobScheduler, config );

        // when
        var executor = executors.executor( ReconcilerRequest.simple(), SYSTEM_DATABASE_NAME );
        executor.execute( this::job );

        // then
        var activeGroups = jobScheduler.activeGroups().map( g -> g.group ).collect( Collectors.toSet() );
        assertThat( "Priority group should be active", activeGroups, contains( Group.DATABASE_RECONCILER_UNBOUND) );
        assertThat( "Normal group shouldn't be active", activeGroups, not( contains( Group.DATABASE_RECONCILER ) ) );
    }

    @Test
    void shouldOnlyUsePriorityExecutorForPriorityDatabases() throws InterruptedException
    {
        // given
        var config = Config.defaults( GraphDatabaseSettings.reconciler_maximum_parallelism, 2 );
        var executors = new ReconcilerExecutors( jobScheduler, config );
        var fooDb = DatabaseIdFactory.from( "foo", UUID.randomUUID() );

        // when/then
        var shouldBePriority = executors.executor( ReconcilerRequest.priority( fooDb ), fooDb.name() );
        shouldBePriority.execute( this::job );

        var activeGroups = jobScheduler.activeGroups().map( g -> g.group ).collect( Collectors.toSet() );
        assertThat( "Priority group should be active", activeGroups, hasItem( Group.DATABASE_RECONCILER_UNBOUND) );
        assertThat( "Normal group shouldn't be active", activeGroups, not( hasItem( Group.DATABASE_RECONCILER ) ) );

        // when/then
        var shouldNotBePriority = executors.executor( ReconcilerRequest.priority( fooDb ), "unknown" );
        shouldNotBePriority.execute( this::job );

        activeGroups = jobScheduler.activeGroups().map( g -> g.group ).collect( Collectors.toSet() );
        assertThat( "Normal group should be active", activeGroups, hasItem( Group.DATABASE_RECONCILER ) );
    }

    private void job()
    {
        try
        {
            semaphore.acquire();
        }
        catch ( InterruptedException e )
        {
            throw new AssertionError( e );
        }
    }
}
