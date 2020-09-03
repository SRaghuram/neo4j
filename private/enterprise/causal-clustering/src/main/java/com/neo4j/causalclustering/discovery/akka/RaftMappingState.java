/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.discovery.ReplicatedRaftMapping;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;

import static java.lang.String.format;
import static java.util.Collections.emptySet;

public class RaftMappingState
{
    private final Log log;
    private final Set<UUID> serversOnceProvidedMappings = new HashSet<>();
    private final Map<ServerId,Map<DatabaseId,RaftMemberId>> raftMembersByServerAndDatabase = new ConcurrentHashMap<>();

    private volatile Map<RaftMemberId,ServerId> serverByRaftMember;
    private volatile int mappingCounter;

    RaftMappingState( Log log )
    {
        this.log = log;
        this.serverByRaftMember = new HashMap<>();
        this.mappingCounter = 0;
    }

    Set<DatabaseId> update( ReplicatedRaftMapping mapping )
    {
        var serverId = mapping.serverId();
        var newEntries = mapping.mapping();
        var actualEntries = raftMembersByServerAndDatabase.getOrDefault( serverId, Collections.emptyMap() );
        if ( !Objects.equals( newEntries, actualEntries ) )
        {
            if ( !newEntries.isEmpty() )
            {
                raftMembersByServerAndDatabase.put( serverId, newEntries );
                reverseMap();
                serversOnceProvidedMappings.add( serverId.uuid() );
            }
            else if ( !actualEntries.isEmpty() )
            {
                raftMembersByServerAndDatabase.remove( serverId );
                reverseMap();
            }
            return changedDatabaseIds( serverId, newEntries, actualEntries );
        }
        return emptySet();
    }

    private void reverseMap()
    {
        var mappingCounter = 0;
        var raftMembersByServerAndDatabase = Map.copyOf( this.raftMembersByServerAndDatabase );
        var serverByRaftMember = new HashMap<RaftMemberId,ServerId>();
        for ( var entry : raftMembersByServerAndDatabase.entrySet() )
        {
            mappingCounter += entry.getValue().size();
            entry.getValue().values().forEach( raftMemberId -> serverByRaftMember.put( raftMemberId, entry.getKey() ) );
        }
        this.mappingCounter = mappingCounter;
        this.serverByRaftMember = serverByRaftMember;
    }

    ServerId resolveServerForRaftMember( RaftMemberId raftMemberId )
    {
        var result = serverByRaftMember.get( raftMemberId );
        return result == null ? fallbackServerForRaftMember( raftMemberId.uuid() ) : result;
    }

    RaftMemberId resolveRaftMemberForServer( DatabaseId databaseId, ServerId serverId )
    {
        var map = raftMembersByServerAndDatabase.get( serverId );
        var result = map == null ? null : map.get( databaseId );
        return result == null ? fallbackRaftMemberForServer( serverId.uuid() ) : result;
    }

    void clearRemoteData( ServerId localServerId )
    {
        raftMembersByServerAndDatabase.entrySet().removeIf( entry -> !entry.getKey().equals( localServerId ) );
        serverByRaftMember.entrySet().removeIf( entry -> !entry.getValue().equals( localServerId ) );
        log.debug( "All remote mappings removed (local=%s) mappings remaining: ", localServerId, serverByRaftMember.size() );
    }

    private ServerId fallbackServerForRaftMember( UUID uuid )
    {
        return serversOnceProvidedMappings.contains( uuid ) ? null : new ServerId( uuid );
    }

    private RaftMemberId fallbackRaftMemberForServer( UUID uuid )
    {
        return serversOnceProvidedMappings.contains( uuid ) ? null : new RaftMemberId( uuid );
    }

    private HashSet<DatabaseId> changedDatabaseIds( ServerId serverId,
            Map<DatabaseId,RaftMemberId> newEntries, Map<DatabaseId,RaftMemberId> actualEntries )
    {
        var removedEntries = new HashMap<>( actualEntries );
        removedEntries.keySet().removeAll( newEntries.keySet() );
        var changedDatabaseIds = new HashSet<>( removedEntries.keySet() );

        var addedOrChangedEntries = new HashMap<>( newEntries );
        addedOrChangedEntries.keySet().removeIf( key -> actualEntries.containsKey( key ) && newEntries.get( key ).equals( actualEntries.get( key ) ) );
        changedDatabaseIds.addAll( addedOrChangedEntries.keySet() );

        logRaftMappingChange( serverId, removedEntries.keySet(), addedOrChangedEntries );

        return changedDatabaseIds;
    }

    private void logRaftMappingChange( ServerId serverId, Set<DatabaseId> removedEntries, Map<DatabaseId,RaftMemberId> addedOrChangedEntries )
    {
        if ( addedOrChangedEntries.isEmpty() )
        {
            log.debug( "Raft Mapping changed for server %s total number of mappings now is %d, nothing added%s", serverId, mappingCounter,
                    removedEntries.isEmpty() ? ", nothing removed" : format( "%n   removed mappings %s", removedEntries ) );
        }
        else
        {
            log.debug( "Raft Mapping changed for server %s total number of mappings now is %d%s%n     added mappings %s", serverId, mappingCounter,
                       removedEntries.isEmpty() ? ", nothing removed" : format( "%n   removed mappings %s", removedEntries ), addedOrChangedEntries );
        }
    }
}
