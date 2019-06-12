/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.identity.MemberId;

import java.util.Set;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.DatabaseId;

import static java.util.stream.Collectors.toUnmodifiableSet;

public class DefaultDiscoveryMember implements DiscoveryMember
{
    private final MemberId id;
    private final DatabaseManager<?> databaseManager;

    public DefaultDiscoveryMember( MemberId id, DatabaseManager<?> databaseManager )
    {
        this.id = id;
        this.databaseManager = databaseManager;
    }

    @Override
    public MemberId id()
    {
        return id;
    }

    @Override
    public Set<DatabaseId> startedDatabases()
    {
        return databaseManager.registeredDatabases()
                .values()
                .stream()
                .filter( ctx -> !ctx.isFailed() && ctx.database().isStarted() )
                .map( ctx -> ctx.database().getDatabaseId() )
                .collect( toUnmodifiableSet() );
    }

    @Override
    public String toString()
    {
        return "DefaultDiscoveryMember{" +
               "id=" + id +
               ", startedDatabases=" + startedDatabases() +
               '}';
    }
}
