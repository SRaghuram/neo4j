/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;

class DatabasePenalties
{
    private final Map<ServerId,ExpiringSet<DatabaseId>> serverSuspensions = new ConcurrentHashMap<>();
    private final Duration suspensionTime;
    private final Clock clock;

    DatabasePenalties( Duration suspensionTime, Clock clock )
    {
        this.clock = clock;
        this.suspensionTime = suspensionTime;
    }

    void issuePenalty( ServerId server, NamedDatabaseId namedDatabaseId )
    {
        serverSuspensions.compute( server, ( serverId, suspendedDatabases ) ->
        {
            suspendedDatabases = suspendedDatabases == null ? new ExpiringSet<>( suspensionTime, clock ) : suspendedDatabases;
            suspendedDatabases.add( namedDatabaseId.databaseId() );
            return suspendedDatabases;
        } );
        dropEmptyMembers();
    }

    private void dropEmptyMembers()
    {
        for ( ServerId server : serverSuspensions.keySet() )
        {
            serverSuspensions.computeIfPresent( server, ( serverId, suspendedDatabases ) -> suspendedDatabases.isEmpty() ? null : suspendedDatabases );
        }
    }

    public boolean notSuspended( DatabaseId databaseId, ServerId server )
    {
        var suspendedDatabases = serverSuspensions.get( server );
        return !(suspendedDatabases != null && suspendedDatabases.contains( databaseId ));
    }
}
