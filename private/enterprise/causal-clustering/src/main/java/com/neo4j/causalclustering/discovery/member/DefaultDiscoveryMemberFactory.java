/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.member;

import java.util.Map;
import java.util.Set;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

import static java.util.stream.Collectors.toUnmodifiableSet;

public class DefaultDiscoveryMemberFactory implements DiscoveryMemberFactory
{
    private final DatabaseManager<?> databaseManager;
    private final DatabaseStateService databaseStateService;

    public DefaultDiscoveryMemberFactory( DatabaseManager<?> databaseManager, DatabaseStateService databaseStateService )
    {
        this.databaseManager = databaseManager;
        this.databaseStateService = databaseStateService;
    }

    @Override
    public DiscoveryMember create( ServerId id )
    {
        var startedDatabases = startedDatabases();
        return new DefaultDiscoveryMember( id, startedDatabases );
    }

    private Set<NamedDatabaseId> startedDatabases()
    {
        return databaseManager.registeredDatabases().entrySet().stream()
                              .filter( this::startedNotFailed )
                              .map( Map.Entry::getKey )
                              .collect( toUnmodifiableSet() );
    }

    private boolean startedNotFailed( Map.Entry<NamedDatabaseId, ? extends DatabaseContext> entry )
    {
        var ctx = entry.getValue();
        var id = entry.getKey();
        return !hasFailed( id ) && ctx.database().isStarted();
    }

    private boolean hasFailed( NamedDatabaseId namedDatabaseId )
    {
        return databaseStateService.causeOfFailure( namedDatabaseId ).isPresent();
    }
}
