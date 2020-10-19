/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.core.consensus.schedule.Timeout;
import com.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory;
import com.neo4j.causalclustering.core.consensus.schedule.Timer;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;
import com.neo4j.causalclustering.discovery.Topology;
import com.neo4j.causalclustering.identity.MemberId;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.util.VisibleForTesting;

import static java.lang.String.format;
import static java.lang.System.lineSeparator;
import static org.neo4j.internal.helpers.Strings.printMap;

public class TopologyLogger
{
    private static final Duration BATCH_TIME = Duration.ofMillis( 1000 );
    private static final String BATCH_TIMER_NAME = "BATCH_LOG_TIMER";

    private final Timer timer;
    private final Supplier<Set<DatabaseId>> allDatabaseSupplier;
    private final Log log;
    private final Timeout batchTimeout;

    private volatile TopologyChange batchKey;
    private volatile List<DatabaseId> batchedDatabaseIds;

    public TopologyLogger( TimerService timerService, LogProvider logProvider, Class<?> loggingClass,
            Supplier<Set<DatabaseId>> allDatabaseSupplier )
    {
        this( timerService, logProvider, loggingClass, allDatabaseSupplier, BATCH_TIME );
    }

    @VisibleForTesting
    TopologyLogger(  TimerService timerService, LogProvider logProvider, Class<?> loggingClass,
            Supplier<Set<DatabaseId>> allDatabaseSupplier, Duration batchTime )
    {
        this.timer = timerService.create( () -> BATCH_TIMER_NAME, Group.TOPOLOGY_LOGGER, unused -> flushTopologyChange() );
        this.allDatabaseSupplier = allDatabaseSupplier;
        this.log = logProvider.getLog( loggingClass );
        this.batchKey = TopologyChange.EMPTY;
        this.batchedDatabaseIds = new ArrayList<>();
        this.batchTimeout = TimeoutFactory.fixedTimeout( batchTime.toMillis(), TimeUnit.MILLISECONDS );
    }

    public void logTopologyChange( String topologyDescription, Topology<?> newTopology, Topology<?> oldTopology )
    {
        var topologyChange = computeTopologyChange( topologyDescription, newTopology, oldTopology );
        topologyChange.ifPresent( tc -> handleTopologyChange( tc, newTopology.databaseId() ) );
    }

    private synchronized void handleTopologyChange( TopologyChange topologyChange, DatabaseId databaseId )
    {
        if ( !Objects.equals( batchKey, topologyChange ) )
        {
            flushTopologyChange();
            batchKey = topologyChange;
            timer.set( batchTimeout );
        }
        batchedDatabaseIds.add( databaseId );
    }

    private Optional<TopologyChange> computeTopologyChange( String topologyDescription,
                                                            Topology<?> newTopology,
                                                            Topology<?> oldTopology )
    {
        var currentMembers = Collections.unmodifiableSet( newTopology.servers().keySet() );

        var lostMembers = new HashSet<>( oldTopology.servers().keySet() );
        lostMembers.removeAll( currentMembers );

        var newMembers = new HashMap<>( newTopology.servers() );
        newMembers.keySet().removeAll( oldTopology.servers().keySet() );

        return !newMembers.isEmpty() || !lostMembers.isEmpty() ?
               Optional.of( new TopologyChange( topologyDescription, currentMembers, lostMembers, newMembers ) ) :
               Optional.empty();
    }

    private void flushTopologyChange()
    {
        TopologyChange currentTopologyChange;
        List<DatabaseId> databaseIds;
        synchronized ( this )
        {
            // Bail out early if there are no topology changes to log
            if ( batchKey == TopologyChange.EMPTY )
            {
                return;
            }
            else
            {
                currentTopologyChange = batchKey;
                batchKey = TopologyChange.EMPTY;

                databaseIds = batchedDatabaseIds;
                batchedDatabaseIds = new ArrayList<>();
            }
        }

        printLogLines( log, currentTopologyChange, databaseIds, allDatabaseSupplier );
    }

    private static void printLogLines( Log log, TopologyChange tc, List<DatabaseId> dbs, Supplier<Set<DatabaseId>> supplier )
    {
        // allDatabases is the superset of databases in GlobalTopologyState at print time and all databases seen during batch
        var allDatabases = new HashSet<>( supplier.get() );
        allDatabases.addAll( dbs );

        var logLine = new StringBuilder();
        if ( dbs.size() == 1 )
        {
            logLine.append( format( "%s for database %s is now: %s", tc.topologyDescription, dbsToReadableString( dbs ),
                                    tc.currentMembers.isEmpty() ? "empty" : membersToStableString( tc.currentMembers ) ) );
        }
        else if ( allDatabases.size() == dbs.size() )
        {
            logLine.append( format( "%s for all databases is now: %s", tc.topologyDescription,
                                    tc.currentMembers.isEmpty() ? "empty" : membersToStableString( tc.currentMembers ) ) );
        }
        else if ( allDatabases.size() - dbs.size() <= 5 && allDatabases.size() < ( 2 * dbs.size() ) )
        {
            var unaffectedDatabases = allDatabases.stream().filter( db -> !dbs.contains( db ) ).collect( Collectors.toList() );
            logLine.append( format( "%s for all databases except for %s is now: %s", tc.topologyDescription, dbsToReadableString( unaffectedDatabases ),
                                    tc.currentMembers.isEmpty() ? "empty" : membersToStableString( tc.currentMembers ) ) );
        }
        else if ( dbs.size() <= 5 )
        {
            logLine.append( format( "%s for databases %s is now: %s", tc.topologyDescription, dbsToReadableString( dbs ),
                                    tc.currentMembers.isEmpty() ? "empty" : membersToStableString( tc.currentMembers ) ) );
        }
        else
        {
            logLine.append( format( "%s for %d databases is now: %s", tc.topologyDescription, dbs.size(),
                                    tc.currentMembers.isEmpty() ? "empty" : membersToStableString( tc.currentMembers ) ) );
        }
        logLine.append( lineSeparator() )
               .append( tc.lostMembers.isEmpty() ? "No members where lost" : format( "Lost members :%s", membersToStableString( tc.lostMembers ) ) )
               .append( lineSeparator() )
               .append( tc.newMembers.isEmpty() ? "No new members" :
                        format( "New members: %s%s", newPaddedLine(), memberInfosToStableString( tc.newMembers ) ) );
        log.info( logLine.toString() );
    }

    private static String newPaddedLine()
    {
        return lineSeparator() + "  ";
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

    private static <V extends DiscoveryServerInfo> String memberInfosToStableString( Map<MemberId,V> memberInfos )
    {
        var sortedMap = new TreeMap<MemberId,V>( Comparator.comparing( MemberId::getUuid ) );
        sortedMap.putAll( memberInfos );
        return printMap( sortedMap, newPaddedLine() );
    }

    private static String membersToStableString( Set<MemberId> members )
    {
        var sortedSet = new TreeSet<>( Comparator.comparing( MemberId::getUuid ) );
        sortedSet.addAll( members );
        return sortedSet.toString();
    }

    private static class TopologyChange
    {
        static final TopologyChange EMPTY = new TopologyChange( "", Set.of(), Set.of(), Map.of() );

        String topologyDescription;
        Set<MemberId> currentMembers;
        Set<MemberId> lostMembers;
        Map<MemberId,? extends DiscoveryServerInfo> newMembers;

        TopologyChange( String topologyDescription,
                        Set<MemberId> currentMembers,
                        Set<MemberId> lostMembers,
                        Map<MemberId,? extends DiscoveryServerInfo> newMembers )
        {
            this.topologyDescription = topologyDescription;
            this.currentMembers = currentMembers;
            this.lostMembers = lostMembers;
            this.newMembers = newMembers;
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
            TopologyChange that = (TopologyChange) o;
            return Objects.equals( topologyDescription, that.topologyDescription ) &&
                   Objects.equals( currentMembers, that.currentMembers ) &&
                   Objects.equals( lostMembers, that.lostMembers ) &&
                   Objects.equals( newMembers, that.newMembers );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( topologyDescription, currentMembers, lostMembers, newMembers );
        }
    }
}
