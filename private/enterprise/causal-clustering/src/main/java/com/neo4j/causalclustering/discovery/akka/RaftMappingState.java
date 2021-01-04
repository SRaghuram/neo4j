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
    // when set it means that the server has been upgraded to 4.3, and thus we no longer allow the fallbacks for UUIDs
    private final Set<UUID> serversOnceProvidedMappings = ConcurrentHashMap.newKeySet();
    private final Map<ServerId,Map<DatabaseId,RaftMemberId>> raftMappingsByServer = new ConcurrentHashMap<>();

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
        var newMap = mapping.databaseToRaftMap();
        var currentMap = raftMappingsByServer.getOrDefault( serverId, Collections.emptyMap() );
        if ( Objects.equals( newMap, currentMap ) )
        {
            return emptySet();
        }
        if ( !newMap.isEmpty() )
        {
            raftMappingsByServer.put( serverId, newMap );
            makeReverseMap();
            serversOnceProvidedMappings.add( serverId.uuid() );
        }
        else if ( !currentMap.isEmpty() )
        {
            raftMappingsByServer.remove( serverId );
            makeReverseMap();
        }
        return changedDatabaseIds( serverId, currentMap, newMap );
    }

    private void makeReverseMap()
    {
        var mappingCounter = 0;
        var raftMappingsByServer = Map.copyOf( this.raftMappingsByServer );
        var serverByRaftMember = new HashMap<RaftMemberId,ServerId>();
        for ( var entry : raftMappingsByServer.entrySet() )
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
        var raftByDatabaseId = raftMappingsByServer.get( serverId );
        var raftMemberId = raftByDatabaseId == null ? null : raftByDatabaseId.get( databaseId );
        return raftMemberId == null ? fallbackRaftMemberForServer( serverId.uuid() ) : raftMemberId;
    }

    void clearRemoteData( ServerId localServerId )
    {
        raftMappingsByServer.entrySet().removeIf( entry -> !entry.getKey().equals( localServerId ) );
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

    private HashSet<DatabaseId> changedDatabaseIds( ServerId serverId, Map<DatabaseId,RaftMemberId> currentMap, Map<DatabaseId,RaftMemberId> newMap )
    {
        var removedEntries = new HashMap<>( currentMap );
        removedEntries.keySet().removeAll( newMap.keySet() );
        var changedDatabaseIds = new HashSet<>( removedEntries.keySet() );

        var addedOrChangedEntries = new HashMap<>( newMap );
        addedOrChangedEntries.entrySet().removeIf( entry -> hasEqualEntry( entry, currentMap ) );
        changedDatabaseIds.addAll( addedOrChangedEntries.keySet() );

        logRaftMappingChange( serverId, removedEntries.keySet(), addedOrChangedEntries );

        return changedDatabaseIds;
    }

    private static <K, V> boolean hasEqualEntry( Map.Entry<K,V> entry, Map<K,V> map )
    {
        return map.containsKey( entry.getKey() ) && entry.getValue().equals( map.get( entry.getKey() ) );
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
