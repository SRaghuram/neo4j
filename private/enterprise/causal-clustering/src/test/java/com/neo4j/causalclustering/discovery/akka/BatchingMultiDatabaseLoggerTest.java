/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.FakeClockJobScheduler;

import static java.lang.String.format;
import static org.neo4j.logging.LogAssertions.assertThat;

class BatchingMultiDatabaseLoggerTest
{
    private static final Duration BATCH_TIME = Duration.ofMillis( 1000 );
    private static final String DESCRIPTION = "test change description";
    private static final String KEY_TITLE_1_2 = "newInt = 1, oldInt = 2";
    private static final String KEY_TITLE_2_3 = "newInt = 2, oldInt = 3";

    @Test
    void shouldLogAndFlushAfterTimeout()
    {
        //setup
        var allDbIds = generateDbIds( 3 );
        var scheduler = new FakeClockJobScheduler();
        var timerService = new TimerService( scheduler, NullLogProvider.getInstance() );
        var logProvider = new AssertableLogProvider();
        var changeLogger = new StubBatchingMultiDatabaseLogger( timerService, logProvider, GlobalTopologyState.class, () -> allDbIds, BATCH_TIME,
                                                                StubBatchingMultiDatabaseLogger.StubChangeKey.EMPTY );

        // given some changes
        allDbIds.forEach( dbId -> changeLogger.logChange( DESCRIPTION, Pair.of( dbId, 1 ), Pair.of( dbId, 2 ) ) );

        // when the timer is fired
        assertThat( logProvider ).doesNotHaveAnyLogs();
        scheduler.forward( BATCH_TIME.plusMillis( 1 ) );

        // then those topology changes should be logged
        var expectedMessage = format( "The %s for all databases %s", DESCRIPTION, KEY_TITLE_1_2 );
        assertThat( logProvider )
                .forClass( GlobalTopologyState.class )
                .forLevel( AssertableLogProvider.Level.INFO )
                .containsMessages( expectedMessage );

        // when the batch time elapses again
        logProvider.clear();
        scheduler.forward( BATCH_TIME.plusSeconds( 2 ) );

        // then nothing should be logged
        assertThat( logProvider ).doesNotHaveAnyLogs();
    }

    @Test
    void newBatchKeyShouldTriggerFlush()
    {
        //setup
        var allDbIds = generateDbIds( 5 );
        var scheduler = new FakeClockJobScheduler();
        var timerService = new TimerService( scheduler, NullLogProvider.getInstance() );
        var logProvider = new AssertableLogProvider();
        var changeLogger = new StubBatchingMultiDatabaseLogger( timerService, logProvider, GlobalTopologyState.class, () -> allDbIds, BATCH_TIME,
                                                                StubBatchingMultiDatabaseLogger.StubChangeKey.EMPTY );

        // when batching with one batchKey
        var loggedDbIds = new ArrayList<DatabaseId>( 2 );
        allDbIds.stream()
                .limit( 2 )
                .peek( loggedDbIds::add )
                .forEach( dbId -> changeLogger.logChange( DESCRIPTION, Pair.of( dbId, 1 ), Pair.of( dbId, 2 ) ) );

        // then nothing should be logged
        assertThat( logProvider ).doesNotHaveAnyLogs();

        // when a new batchKey is submitted
        allDbIds.stream()
                .skip( 2 )
                .limit( 2 )
                .peek( loggedDbIds::add )
                .forEach( dbId -> changeLogger.logChange( DESCRIPTION, Pair.of( dbId, 2 ), Pair.of( dbId, 3 ) ) );

        // then first batchKey should be flushed
        var expectedMessage = format( "The %s for databases %s and %s %s",
                                      DESCRIPTION, loggedDbIds.get( 0 ), loggedDbIds.get( 1 ), KEY_TITLE_1_2 );
        assertThat( logProvider )
                .forClass( GlobalTopologyState.class )
                .forLevel( AssertableLogProvider.Level.INFO )
                .containsMessages( expectedMessage );
    }

    @Test
    void shouldLogOneExplicitDBName()
    {
        //setup
        var allDbIds = generateDbIds( 3 );
        var scheduler = new FakeClockJobScheduler();
        var timerService = new TimerService( scheduler, NullLogProvider.getInstance() );
        var logProvider = new AssertableLogProvider();
        var changeLogger = new StubBatchingMultiDatabaseLogger( timerService, logProvider, GlobalTopologyState.class, () -> allDbIds, BATCH_TIME,
                                                                StubBatchingMultiDatabaseLogger.StubChangeKey.EMPTY );

        // when timer is fired after topology change for one database
        var dbId = allDbIds.stream().findFirst().get();
        changeLogger.logChange( DESCRIPTION, Pair.of( dbId, 1 ), Pair.of( dbId, 2 ) );
        scheduler.forward( BATCH_TIME.plusMillis( 1 ) );

        // then log should contain one explicit database name
        var expectedMessage = format( "The %s for database %s %s", DESCRIPTION, dbId, KEY_TITLE_1_2 );
        assertThat( logProvider )
                .forClass( GlobalTopologyState.class )
                .forLevel( AssertableLogProvider.Level.INFO )
                .containsMessages( expectedMessage );
    }

    @Test
    void shouldLogAllDatabasesBatchedTogether()
    {
        //setup
        var allDbIds = generateDbIds( 3 );
        var scheduler = new FakeClockJobScheduler();
        var timerService = new TimerService( scheduler, NullLogProvider.getInstance() );
        var logProvider = new AssertableLogProvider();
        var changeLogger = new StubBatchingMultiDatabaseLogger( timerService, logProvider, GlobalTopologyState.class, () -> allDbIds, BATCH_TIME,
                                                                StubBatchingMultiDatabaseLogger.StubChangeKey.EMPTY );
        // when timer is fired after topology change for all database
        allDbIds.forEach( dbId -> changeLogger.logChange( DESCRIPTION, Pair.of( dbId, 1 ), Pair.of( dbId, 2 ) ) );
        scheduler.forward( BATCH_TIME.plusMillis( 1 ) );

        // then log should contain changes for "all databases"
        var expectedMessage = format( "The %s for all databases %s", DESCRIPTION, KEY_TITLE_1_2 );
        assertThat( logProvider )
                .forClass( GlobalTopologyState.class )
                .forLevel( AssertableLogProvider.Level.INFO )
                .containsMessages( expectedMessage );
    }

    @Test
    void shouldLogWithDatabasesNotChanged()
    {
        //setup
        var allDbIds = generateDbIds( 5 );
        var scheduler = new FakeClockJobScheduler();
        var timerService = new TimerService( scheduler, NullLogProvider.getInstance() );
        var logProvider = new AssertableLogProvider();
        var changeLogger = new StubBatchingMultiDatabaseLogger( timerService, logProvider, GlobalTopologyState.class, () -> allDbIds, BATCH_TIME,
                                                                StubBatchingMultiDatabaseLogger.StubChangeKey.EMPTY );

        // when timer is fired after topology change for 4 out of 5 databases
        var unchangedDbId = allDbIds.stream().findFirst().get();
        allDbIds.stream()
                .skip( 1 )
                .forEach( dbId -> changeLogger.logChange( DESCRIPTION, Pair.of( dbId, 1 ), Pair.of( dbId, 2 ) ) );
        scheduler.forward( BATCH_TIME.plusMillis( 1 ) );

        // then log should contain changes for "all databases except..."
        var expectedMessage = format( "The %s for all databases except for %s %s", DESCRIPTION, unchangedDbId, KEY_TITLE_1_2 );
        assertThat( logProvider )
                .forClass( GlobalTopologyState.class )
                .forLevel( AssertableLogProvider.Level.INFO )
                .containsMessages( expectedMessage );
    }

    @Test
    void shouldLogExplicitNamesForChangedDatabases()
    {
        //setup
        var allDbIds = generateDbIds( 8 );
        var scheduler = new FakeClockJobScheduler();
        var timerService = new TimerService( scheduler, NullLogProvider.getInstance() );
        var logProvider = new AssertableLogProvider();
        var changeLogger = new StubBatchingMultiDatabaseLogger( timerService, logProvider, GlobalTopologyState.class, () -> allDbIds, BATCH_TIME,
                                                                StubBatchingMultiDatabaseLogger.StubChangeKey.EMPTY );

        // when timer is fired after topology change for 2 out of 8 databases
        var changedDbIds = new ArrayList<DatabaseId>( 2 );
        allDbIds.stream()
                .limit( 2 )
                .peek( changedDbIds::add )
                .forEach( dbId -> changeLogger.logChange( DESCRIPTION, Pair.of( dbId, 1), Pair.of( dbId, 2 ) ) );
        scheduler.forward( BATCH_TIME.plusMillis( 1 ) );

        // then log should contain explicit names for changed databases
        var expectedMessage = format( "The %s for databases %s and %s %s", DESCRIPTION, changedDbIds.get( 0 ), changedDbIds.get( 1 ), KEY_TITLE_1_2 );
        assertThat( logProvider )
                .forClass( GlobalTopologyState.class )
                .forLevel( AssertableLogProvider.Level.INFO )
                .containsMessages( expectedMessage );
    }

    @Test
    void shouldLogNumberOfDatabasesChanged()
    {
        //setup
        final int NBR_CHANGED_DATABASES = 8;

        var allDbIds = generateDbIds( 15 );
        var scheduler = new FakeClockJobScheduler();
        var timerService = new TimerService( scheduler, NullLogProvider.getInstance() );
        var logProvider = new AssertableLogProvider();
        var changeLogger = new StubBatchingMultiDatabaseLogger( timerService, logProvider, GlobalTopologyState.class, () -> allDbIds, BATCH_TIME,
                                                                StubBatchingMultiDatabaseLogger.StubChangeKey.EMPTY );

        // when timer is fired after topology change for 8 out of 15 databases
        allDbIds.stream()
                .limit( NBR_CHANGED_DATABASES )
                .forEach( dbId -> changeLogger.logChange( DESCRIPTION, Pair.of( dbId, 1 ), Pair.of( dbId, 2 ) ) );
        scheduler.forward( BATCH_TIME.plusMillis( 1 ) );

        // then log should contain number of databases changed
        var expectedMessage = format( "The %s for %d databases %s", DESCRIPTION, NBR_CHANGED_DATABASES, KEY_TITLE_1_2 );
        assertThat( logProvider )
                .forClass( GlobalTopologyState.class )
                .forLevel( AssertableLogProvider.Level.INFO )
                .containsMessages( expectedMessage );
    }

    @Test
    void newBatchShouldResetTimeout()
    {
        //setup
        var allDbIds = generateDbIds( 3 );
        var dbIdsList = new ArrayList<>( allDbIds );
        var scheduler = new FakeClockJobScheduler();
        var timerService = new TimerService( scheduler, NullLogProvider.getInstance() );
        var logProvider = new AssertableLogProvider();
        var changeLogger = new StubBatchingMultiDatabaseLogger( timerService, logProvider, GlobalTopologyState.class, () -> allDbIds, BATCH_TIME,
                                                                StubBatchingMultiDatabaseLogger.StubChangeKey.EMPTY );

        // when
        // First batch starts timer for batch time
        changeLogger.logChange( DESCRIPTION, Pair.of( dbIdsList.get( 0 ), 1 ), Pair.of( dbIdsList.get( 0 ), 2 ) );

        // Half the batch time passes
        scheduler.forward( BATCH_TIME.dividedBy( 2 ) );
        // Second batch should reset timer and cause logging of first batch
        changeLogger.logChange( DESCRIPTION, Pair.of( dbIdsList.get( 1 ), 2 ), Pair.of( dbIdsList.get( 1 ), 3 ) );

        // then
        var expectedMessage = format( "The %s for database %s %s", DESCRIPTION, dbIdsList.get( 0 ), KEY_TITLE_1_2 );
        assertThat( logProvider )
                .forClass( GlobalTopologyState.class )
                .forLevel( AssertableLogProvider.Level.INFO )
                .containsMessages( expectedMessage );
        logProvider.clear();

        // when
        // Additional log in second batch
        changeLogger.logChange( DESCRIPTION, Pair.of( dbIdsList.get( 2 ), 2 ), Pair.of( dbIdsList.get( 2 ), 3 ) );

        // Second half (plus 1 milli) of original batch time passes and the internal timer elapses
        scheduler.forward( BATCH_TIME.dividedBy( 2 ).plusMillis( 1 ) );

        // then
        // Neither additional log in second batch, nor elapse of original timer should cause logging
        assertThat( logProvider ).doesNotHaveAnyLogs();

        // when
        // Second half of batch time (after timer reset) passes and internal timer elapses again
        scheduler.forward( BATCH_TIME.dividedBy( 2 ) );

        //then
        expectedMessage = format( "The %s for all databases except for %s %s", DESCRIPTION, dbIdsList.get( 0 ), KEY_TITLE_2_3 );
        assertThat( logProvider )
                .forClass( GlobalTopologyState.class )
                .forLevel( AssertableLogProvider.Level.INFO )
                .containsMessages( expectedMessage );
    }

    private Set<DatabaseId> generateDbIds( int nbrOfBds )
    {
        return Stream.generate( TestDatabaseIdRepository::randomDatabaseId ).limit( nbrOfBds ).collect( sortedSetCollector( DatabaseId::uuid ) );
    }

    public static <T, U extends Comparable<? super U>> Collector<T,?,SortedSet<T>> sortedSetCollector( Function<? super T, ? extends U> comparing )
    {
        return Collectors.toCollection( () -> new TreeSet<>( Comparator.comparing( comparing ) ) );
    }

    private static class StubBatchingMultiDatabaseLogger extends BatchingMultiDatabaseLogger<Pair<DatabaseId,Integer>>
    {
        protected StubBatchingMultiDatabaseLogger( TimerService timerService, LogProvider logProvider,
                                                   Class<?> loggingClass, Supplier<Set<DatabaseId>> allDatabaseSupplier, Duration batchTime,
                                                   ChangeKey emptyBatchKey )
        {
            super( timerService, logProvider, loggingClass, allDatabaseSupplier, batchTime, emptyBatchKey );
        }

        @Override
        protected Optional<ChangeKey> computeChange( String changeDescription, Pair<DatabaseId,Integer> newInfo, Pair<DatabaseId,Integer> oldInfo )
        {
            return Optional.of( new StubChangeKey( changeDescription, newInfo.other(), oldInfo.other() ) );
        }

        @Override
        protected DatabaseId extractDatabaseId( Pair<DatabaseId,Integer> info )
        {
            return info.first();
        }

        static class StubChangeKey implements BatchingMultiDatabaseLogger.ChangeKey
        {
            static final StubChangeKey EMPTY = new StubChangeKey( "", null, null );

            private final String description;
            private final Integer newInt;
            private final Integer oldInt;

            StubChangeKey( String description, Integer newInt, Integer oldInt )
            {

                this.description = description;
                this.newInt = newInt;
                this.oldInt = oldInt;
            }

            @Override
            public String title()
            {
                return description;
            }

            @Override
            public String specification()
            {
                return  "newInt = " + newInt + ", oldInt = " + oldInt;
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }
                StubChangeKey that = (StubChangeKey) o;
                return Objects.equals( description, that.description ) &&
                       Objects.equals( newInt, that.newInt ) &&
                       Objects.equals( oldInt, that.oldInt );
            }

            @Override
            public int hashCode()
            {
                return Objects.hash( description, newInt, oldInt );
            }
        }
    }

}
