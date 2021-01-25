/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;

public class InMemoryCoreServerIdentity implements CoreServerIdentity
{
    private final ServerId serverId;
    private final Map<DatabaseId,RaftMemberId> raftMemberIds = new ConcurrentHashMap<>();

    public InMemoryCoreServerIdentity()
    {
        this.serverId = IdFactory.randomServerId();
    }

    public InMemoryCoreServerIdentity( long start )
    {
        this.serverId = new ServerId( new UUID( start << 48, 0 ) );
    }

    @Override
    public ServerId serverId()
    {
        return serverId;
    }

    @Override
    public RaftMemberId raftMemberId( DatabaseId databaseId )
    {
        return raftMemberIds.computeIfAbsent( databaseId, ignored -> new RaftMemberId( generateRaftMemberUUID() ) );
    }

    @Override
    public RaftMemberId raftMemberId( NamedDatabaseId namedDatabaseId )
    {
        return raftMemberId( namedDatabaseId.databaseId() );
    }

    @Override
    public void createMemberId( NamedDatabaseId databaseId, RaftMemberId raftMemberId )
    {
        if ( raftMemberIds.get( databaseId.databaseId() ) != null )
        {
            throw new IllegalStateException();
        }
        raftMemberIds.put( databaseId.databaseId(), raftMemberId );
    }

    @Override
    public RaftMemberId loadMemberId( NamedDatabaseId databaseId )
    {
        return raftMemberIds.get( databaseId.databaseId() );
    }

    private UUID generateRaftMemberUUID()
    {
        if ( serverId.uuid().getLeastSignificantBits() != 0 )
        {
            return UUID.randomUUID();
        }
        var baseId = serverId.uuid().getMostSignificantBits();
        return new UUID( baseId + ((1L + raftMemberIds.size()) << 32), 0L );
    }
}
