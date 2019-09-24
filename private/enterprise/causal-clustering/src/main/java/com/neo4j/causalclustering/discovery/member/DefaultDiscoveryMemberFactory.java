/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.member;

import com.neo4j.causalclustering.identity.MemberId;

import java.util.Set;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;

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
                .map( DatabaseContext::database )
                .filter( db -> !hasFailed( db.getDatabaseId() ) && db.isStarted() )
                .map( Database::getDatabaseId )
                .collect( toUnmodifiableSet() );
    }

    private boolean hasFailed( DatabaseId databaseId )
    {
        return databaseStateService.causeOfFailure( databaseId ).isPresent();
    }
}
