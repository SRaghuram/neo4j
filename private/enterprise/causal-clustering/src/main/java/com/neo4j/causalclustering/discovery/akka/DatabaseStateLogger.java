/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.LogProvider;

import static java.lang.System.lineSeparator;

public class DatabaseStateLogger extends BatchingMultiDatabaseLogger<ReplicatedDatabaseState>
{
    public DatabaseStateLogger( TimerService timerService, LogProvider logProvider, Class<?> loggingClass,
                                Supplier<Set<DatabaseId>> allDatabaseSupplier )
    {
        super( timerService, logProvider, loggingClass, allDatabaseSupplier, BATCH_TIME, StateChange.EMPTY );
    }

    @Override
    protected Optional<ChangeKey> computeChange( String changeDescription, ReplicatedDatabaseState newInfo, ReplicatedDatabaseState oldInfo )
    {
        var newServerStates = newInfo.memberStates();
        var oldServerStates = oldInfo.memberStates();

        var updatedServersBefore = new HashMap<ServerId,DiscoveryDatabaseState>();
        var updatedServersAfter = new HashMap<ServerId,DiscoveryDatabaseState>();

        for ( var entry : newServerStates.entrySet() )
        {
            var serverId = entry.getKey();
            var newState = entry.getValue();
            var oldState = oldServerStates.get( entry.getKey() );
            if ( oldServerStates.containsKey( serverId ) && !Objects.equals( newState, oldState ) )
            {
                updatedServersAfter.put( serverId, newState );
                updatedServersBefore.put( serverId, oldState );
            }
        }

        return !updatedServersAfter.isEmpty() ?
               Optional.of( new StateChange( changeDescription, updatedServersBefore, updatedServersAfter ) ) :
               Optional.empty();
    }

    @Override
    protected DatabaseId extractDatabaseId( ReplicatedDatabaseState info )
    {
        return info.databaseId();
    }

    private static class StateChange implements ChangeKey
    {
        static final StateChange EMPTY = new StateChange( "", Map.of(), Map.of() );
        private final String stateDescription;
        private final Map<ServerId,DiscoveryDatabaseState> updatedServersBefore;
        private final Map<ServerId,DiscoveryDatabaseState> updatedServerAfter;

        StateChange( String stateDescription,
                     Map<ServerId,DiscoveryDatabaseState> updatedServersBefore,
                     Map<ServerId,DiscoveryDatabaseState> updatedServerAfter )
        {

            this.stateDescription = stateDescription;
            this.updatedServersBefore = updatedServersBefore;
            this.updatedServerAfter = updatedServerAfter;
        }

        @Override
        public String title()
        {
            return stateDescription;
        }

        @Override
        public String specification()
        {
            return "changed" + lineSeparator() +
                   "Servers changed their state:" + printUpdatedMembers();
        }

        private String printUpdatedMembers()
        {
            var result = new StringBuilder();
            for ( ServerId serverId : updatedServerAfter.keySet() )
            {
                result.append( newPaddedLine() );
                result.append( "  to: " ).append( serverId ).append( "=" ).append( updatedServerAfter.get( serverId ) ).append( newPaddedLine() );
                result.append( "from: " ).append( serverId ).append( "=" ).append( updatedServersBefore.get( serverId ) );
            }
            return result.toString();
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
            StateChange that = (StateChange) o;
            return Objects.equals( stateDescription, that.stateDescription ) &&
                   Objects.equals( updatedServersBefore, that.updatedServersBefore ) &&
                   Objects.equals( updatedServerAfter, that.updatedServerAfter );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( stateDescription, updatedServersBefore, updatedServerAfter );
        }
    }
}
