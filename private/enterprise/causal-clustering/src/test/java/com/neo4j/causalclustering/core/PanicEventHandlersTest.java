/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.DatabasePanicHandlers;
import com.neo4j.causalclustering.error_handling.DatabasePanicReason;
import com.neo4j.causalclustering.error_handling.DefaultPanicService;
import com.neo4j.causalclustering.error_handling.PanicService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;

import static com.neo4j.causalclustering.error_handling.DatabasePanicEventHandler.panicDbmsIfSystemDatabasePanics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

@ExtendWith( LifeExtension.class )
public class PanicEventHandlersTest
{
    @Inject
    private LifeSupport lifeSupport;

    private final NamedDatabaseId nonSystemDatabaseId = randomNamedDatabaseId();
    private final ConcurrentLinkedQueue<Throwable> dbmsPanics = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<NamedDatabaseId,List<Throwable>> databasePanics = new ConcurrentHashMap<>();
    private final OnDemandJobScheduler scheduler = new OnDemandJobScheduler();
    private final DefaultPanicService panicService =
            new DefaultPanicService( scheduler, NullLogService.getInstance(), panic -> dbmsPanics.add( panic.getCause() ) );

    @BeforeEach
    private void setup()
    {
        lifeSupport.add( scheduler );
        lifeSupport.add( new TestPanicHandlers( panicService, NAMED_SYSTEM_DATABASE_ID ) );
        lifeSupport.add( new TestPanicHandlers( panicService, nonSystemDatabaseId ) );
    }

    @Test
    void systemDatabasePanicShouldPanicDbms()
    {
        // when
        RuntimeException exception = panicDatabase( NAMED_SYSTEM_DATABASE_ID );
        scheduler.runJob();

        // then
        assertThat( dbmsPanics ).containsExactly( exception );
    }

    @Test
    void nonSystemDatabasePanicShouldNotPanicDbms()
    {
        // when
        RuntimeException exception = panicDatabase( nonSystemDatabaseId );
        scheduler.runJob();

        // then
        assertThat( dbmsPanics ).isEmpty();
    }

    @AfterEach
    void checkScheduler()
    {
        assertThat( scheduler.getJob() ).as( "There are no remaining queued tasks on the scheduler" ).isNull();
        scheduler.close();
    }

    private class TestPanicHandlers extends DatabasePanicHandlers
    {
        protected TestPanicHandlers( PanicService panicService,
                                     NamedDatabaseId namedDatabaseId )
        {
            super( panicService, namedDatabaseId, List.of( info ->
                                                                   databasePanics.compute( namedDatabaseId, ( key, list ) ->
                                                                   {
                                                                       list = list == null ? new LinkedList<>() : list;
                                                                       list.add( info.getCause() );
                                                                       return list;
                                                                   } ),
                                                           panicDbmsIfSystemDatabasePanics( panicService.panicker() )
            ) );
        }
    }

    private RuntimeException panicDatabase( NamedDatabaseId databaseId )
    {
        String errorMessage = this.getClass().getCanonicalName();
        RuntimeException exception = new RuntimeException( errorMessage );
        panicService.panickerFor( databaseId ).panic( DatabasePanicReason.Test, exception );
        scheduler.runJob();
        assertThat( databasePanics.get( databaseId ) ).containsExactly( exception );
        return exception;
    }
}
