/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.core.CoreClusterMember;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.function.Predicates;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.test.DbRepresentation;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;

public class DataMatching
{
    private static final int DEFAULT_TIMEOUT_MS = (int) MINUTES.toMillis( 3 );

    private DataMatching()
    {
    }

    public static void dataMatchesEventually( ClusterMember expectedMember, Collection<? extends ClusterMember> targets ) throws TimeoutException
    {
        String databaseName = expectedMember.config().get( default_database );
        dataMatchesEventually( () -> buildDbRepresentation( expectedMember, databaseName ), databaseName, targets );
    }

    public static void dataMatchesEventually( Cluster cluster, String databaseName ) throws TimeoutException
    {
        CoreClusterMember leader = cluster.awaitLeader( databaseName );
        dataMatchesEventually( () -> buildDbRepresentation( leader, databaseName ), databaseName, cluster.allMembers() );
    }

    public static void dataMatchesEventually( DbRepresentation expected, String databaseName, Collection<? extends ClusterMember> targets )
            throws TimeoutException
    {
        dataMatchesEventually( ignored -> expected, databaseName, targets );
    }

    public static void dataMatchesEventually( Function<String,DbRepresentation> expected, String databaseName, Collection<? extends ClusterMember> targets )
            throws TimeoutException
    {
        dataMatchesEventually( () -> expected.apply( databaseName ), databaseName, targets );
    }

    public static void dataMatchesEventually( Supplier<DbRepresentation> expected, String databaseName, Collection<? extends ClusterMember> members )
            throws TimeoutException
    {
        for ( ClusterMember member : members )
        {
            Predicates.await( () -> Objects.equals( expected.get(), buildDbRepresentation( member, databaseName ) ), DEFAULT_TIMEOUT_MS, MILLISECONDS );
        }
    }

    private static DbRepresentation buildDbRepresentation( ClusterMember member, String databaseName )
    {
        try
        {
            var db = member.database( databaseName );
            if ( db == null )
            {
                // cluster member is shutdown
                return null;
            }
            return DbRepresentation.of( db );
        }
        catch ( DatabaseShutdownException e )
        {
            // this can happen if the database is still in the process of starting or doing a store copy
            return null;
        }
    }
}
