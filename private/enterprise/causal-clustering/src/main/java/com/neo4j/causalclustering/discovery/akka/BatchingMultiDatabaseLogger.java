/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.core.consensus.schedule.Timeout;
import com.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory;
import com.neo4j.causalclustering.core.consensus.schedule.Timer;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;

import static java.lang.String.format;
import static java.lang.System.lineSeparator;
import static org.neo4j.internal.helpers.Strings.printMap;

/**
 * Batches logs that are equal for everything except for the database. This can be useful for example when an event happens to a server
 * but is logged on database level. The changes are actually logged when the log content change or after a given time (BATCH_TIME), whichever comes first.
 * In order to use, implementations are needed for the two abstract methods and an object that implements BatchingMultiDatabaseLogger.ChangeKey.
 * Implementing instances are used by calling the logChange method for all detected possible changes.
 * Actual log output will look like:
 *      The [ChangeKey.title()] [info about which databases are batched] [ChangeKey.specification()]
 *
 * @param <T> An object represent the state or info that should be logged
 */

public abstract class BatchingMultiDatabaseLogger<T>
{
    protected static final Duration BATCH_TIME = Duration.ofMillis( 1000 );
    protected static final String BATCH_TIMER_NAME = "BATCH_LOG_TIMER";

    protected final Timer timer;
    protected final Supplier<Set<DatabaseId>> allDatabaseSupplier;
    protected final Log log;
    protected final Timeout batchTimeout;

    protected final ChangeKey emptyKey;
    protected volatile ChangeKey batchKey;
    protected volatile List<DatabaseId> batchedDatabaseIds;

    protected BatchingMultiDatabaseLogger( TimerService timerService, LogProvider logProvider, Class<?> loggingClass,
                                           Supplier<Set<DatabaseId>> allDatabaseSupplier, Duration batchTime, ChangeKey emptyBatchKey )
    {
        this.timer = timerService.create( () -> BATCH_TIMER_NAME, Group.TOPOLOGY_LOGGER, unused -> flushChange() );
        this.allDatabaseSupplier = allDatabaseSupplier;
        this.log = logProvider.getLog( loggingClass );
        this.emptyKey = emptyBatchKey;
        this.batchKey = emptyBatchKey;
        this.batchedDatabaseIds = new ArrayList<>();
        this.batchTimeout = TimeoutFactory.fixedTimeout( batchTime.toMillis(), TimeUnit.MILLISECONDS );
    }

    /**
     * The public method that will actually be called for all changes. These changes will be batched.
     * @param changeDescription a String representation of what is logged
     * @param newInfo the new state of info
     * @param oldInfo the state of info as it was before the log event
     */
    public void logChange( String changeDescription, T newInfo, T oldInfo )
    {
        var change = computeChange( changeDescription, newInfo, oldInfo );
        change.ifPresent( content -> handleTopologyChange( content, extractDatabaseId( newInfo ) ) );
    }

    private synchronized void handleTopologyChange( ChangeKey changeKey, DatabaseId databaseId )
    {
        if ( !Objects.equals( batchKey, changeKey ) )
        {
            flushChange();
            batchKey = changeKey;
            timer.set( batchTimeout );
        }
        batchedDatabaseIds.add( databaseId );
    }

    /**
     * Should calculate and return an object that represents the info that are common to all the entries that
     * will be batched. Should return an empty Optional if there is no relevant differance between newInfo
     * and oldInfo.
     *
     * @param changeDescription the title of the batched log message
     * @param newInfo the new state of info
     * @param oldInfo the state or info as it was before the log event
     * @return an Optional of a ChangeKey that represent the common change that should be logged or an empty
     *          Optional if no relevant change
     */
    protected abstract Optional<ChangeKey> computeChange( String changeDescription, T newInfo, T oldInfo );

    /**
     * Should extract and return the DatabaseId that the current T are referring to
     * @param info an object of typ T
     * @return the DatabaseId for that instance of T
     */
    protected abstract DatabaseId extractDatabaseId( T info );

    private void flushChange()
    {
        ChangeKey currentChangeKey;
        List<DatabaseId> databaseIds;
        synchronized ( this )
        {
            // Bail out early if there are no changes to log
            if ( batchKey.equals( emptyKey ) )
            {
                return;
            }
            else
            {
                currentChangeKey = batchKey;
                batchKey = emptyKey;

                databaseIds = batchedDatabaseIds;
                batchedDatabaseIds = new ArrayList<>();
            }
        }

        printLogLines( log, currentChangeKey, databaseIds, allDatabaseSupplier );
    }

    private static void printLogLines( Log log, ChangeKey key, List<DatabaseId> dbs, Supplier<Set<DatabaseId>> supplier )
    {
        // allDatabases is the superset of databases in GlobalTopologyState at print time and all databases seen during batch
        var allDatabases = new HashSet<>( supplier.get() );
        allDatabases.addAll( dbs );

        var logLine = new StringBuilder();
        if ( dbs.size() == 1 )
        {
            logLine.append( format( "The %s for database %s %s", key.title(), dbsToReadableString( dbs ), key.specification() ) );
        }
        else if ( allDatabases.size() == dbs.size() )
        {
            logLine.append( format( "The %s for all databases %s", key.title(), key.specification() ) );
        }
        else if ( allDatabases.size() - dbs.size() <= 5 && allDatabases.size() < ( 2 * dbs.size() ) )
        {
            var unaffectedDatabases = allDatabases.stream().filter( db -> !dbs.contains( db ) ).collect( Collectors.toList() );
            logLine.append( format( "The %s for all databases except for %s %s",
                                    key.title(), dbsToReadableString( unaffectedDatabases ), key.specification() ) );
        }
        else if ( dbs.size() <= 5 )
        {
            logLine.append( format( "The %s for databases %s %s", key.title(), dbsToReadableString( dbs ), key.specification() ) );
        }
        else
        {
            logLine.append( format( "The %s for %d databases %s", key.title(), dbs.size(), key.specification() ) );
        }
        log.info( logLine.toString() );
    }

    protected static String newPaddedLine()
    {
        return lineSeparator() + "  ";
    }

    protected static <V extends DiscoveryServerInfo> String serverInfosToStableString( Map<ServerId,V> serverInfos )
    {
        var sortedMap = new TreeMap<ServerId,V>( Comparator.comparing( ServerId::uuid ) );
        sortedMap.putAll( serverInfos );
        return printMap( sortedMap, newPaddedLine() );
    }

    protected static String serversToStableString( Set<ServerId> members )
    {
        var sortedSet = new TreeSet<>( Comparator.comparing( ServerId::uuid ) );
        sortedSet.addAll( members );
        return sortedSet.toString();
    }

    private static String dbsToReadableString( List<DatabaseId> dbs )
    {
        if ( dbs.size() == 1 )
        {
            return dbs.get( 0 ).toString();
        }
        else if ( dbs.size() > 1 )
        {
            StringBuilder result = new StringBuilder();
            result.append( dbs.get( 0 ) );
            var lastItem = dbs.size() - 1;
            for ( int i = 1; i < lastItem; i++ )
            {
                result.append( ", " ).append( dbs.get( i ) );
            }
            result.append( " and " ).append( dbs.get( lastItem ) );
            return result.toString();
        }
        return "";
    }

    protected interface ChangeKey
    {
        /**
         * The title of the change that will be logged
         * @return a title
         */
        String title();

        /**
         * The output of the common change that will be logged
         * @return change specification
         */
        String specification();
    }
}
