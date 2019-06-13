/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.member;

import com.neo4j.causalclustering.identity.MemberId;

import java.util.Set;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.DatabaseId;

import static java.util.stream.Collectors.toUnmodifiableSet;

public class DefaultDiscoveryMemberFactory implements DiscoveryMemberFactory
{
    private final DatabaseManager<?> databaseManager;

    public DefaultDiscoveryMemberFactory( DatabaseManager<?> databaseManager )
    {
        this.databaseManager = databaseManager;
    }

    @Override
    public DiscoveryMember create( MemberId id )
    {
        var startedDatabases = startedDatabases();
        return new DefaultDiscoveryMember( id, startedDatabases );
    }

    private Set<DatabaseId> startedDatabases()
    {
        return databaseManager.registeredDatabases()
                .values()
                .stream()
                .filter( ctx -> !ctx.isFailed() && ctx.database().isStarted() )
                .map( ctx -> ctx.database().getDatabaseId() )
                .collect( toUnmodifiableSet() );
    }
}
