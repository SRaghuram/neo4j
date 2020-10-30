/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;
import com.neo4j.causalclustering.discovery.Topology;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.LogProvider;
import org.neo4j.util.VisibleForTesting;

import static java.lang.String.format;
import static java.lang.System.lineSeparator;

public class TopologyLogger extends TopologyStateLogger<Topology<?>>
{
    public TopologyLogger( TimerService timerService, LogProvider logProvider, Class<?> loggingClass,
            Supplier<Set<DatabaseId>> allDatabaseSupplier )
    {
        this( timerService, logProvider, loggingClass, allDatabaseSupplier, BATCH_TIME );
    }

    @VisibleForTesting
    TopologyLogger(  TimerService timerService, LogProvider logProvider, Class<?> loggingClass,
            Supplier<Set<DatabaseId>> allDatabaseSupplier, Duration batchTime )
    {
        super( timerService, logProvider, loggingClass, allDatabaseSupplier, batchTime, TopologyChange.EMPTY );
    }

    @Override
    protected Optional<TopologyChange> computeTopologyChange( String topologyDescription,
                                                            Topology<?> newTopology,
                                                            Topology<?> oldTopology )
    {
        var currentServers = Collections.unmodifiableSet( newTopology.servers().keySet() );

        var lostServers = new HashSet<>( oldTopology.servers().keySet() );
        lostServers.removeAll( currentServers );

        var newServers = new HashMap<>( newTopology.servers() );
        newServers.keySet().removeAll( oldTopology.servers().keySet() );

        return !newServers.isEmpty() || !lostServers.isEmpty() ?
               Optional.of( new TopologyChange( topologyDescription, currentServers, lostServers, newServers ) ) :
               Optional.empty();
    }

    @Override
    protected DatabaseId extractDatabaseId( Topology<?> info )
    {
        return info.databaseId();
    }

    protected static class TopologyChange implements ChangeKey
    {
        static final TopologyChange EMPTY = new TopologyChange( "", Set.of(), Set.of(), Map.of() );

        String topologyDescription;
        Set<ServerId> currentMembers;
        Set<ServerId> lostMembers;
        Map<ServerId,? extends DiscoveryServerInfo> newMembers;

        TopologyChange( String topologyDescription,
                        Set<ServerId> currentMembers,
                        Set<ServerId> lostMembers,
                        Map<ServerId,? extends DiscoveryServerInfo> newMembers )
        {
            this.topologyDescription = topologyDescription;
            this.currentMembers = currentMembers;
            this.lostMembers = lostMembers;
            this.newMembers = newMembers;
        }

        @Override
        public String title()
        {
            return topologyDescription;
        }

        @Override
        public String specification()
        {
            return format( "is now: %s", currentMembers.isEmpty() ? "empty" : serversToStableString( currentMembers ) ) +
                            lineSeparator() +
                            (lostMembers.isEmpty() ? "No servers where lost" :
                             format( "Lost servers :%s", serversToStableString( lostMembers ) )) +
                            lineSeparator() +
                            (newMembers.isEmpty() ? "No new servers" :
                             format( "New servers: %s%s", TopologyStateLogger.newPaddedLine(), serverInfosToStableString( newMembers ) ));
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
