/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream;

import com.neo4j.causalclustering.identity.IdFactory;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

public class UpstreamDatabaseSelectionStrategyTest
{

    @Test
    void upstreamMembersShouldWrapWrapOptionalByDefault() throws UpstreamDatabaseSelectionException
    {
        // given
        var serverId = IdFactory.randomServerId();
        var populatedStrategy = new StubStrategy( serverId );
        var emptyStrategy = new StubStrategy( null );

        // when
        var nonEmptyMemberIds = populatedStrategy.upstreamServersForDatabase( null ); // databaseId ignored
        var emptyMemberIds = emptyStrategy.upstreamServersForDatabase( null );

        // then
        assertThat( nonEmptyMemberIds, hasSize( 1 ) );
        assertThat( nonEmptyMemberIds, contains( serverId ) );
        assertThat( emptyMemberIds, empty() );
    }

    private static class StubStrategy extends UpstreamDatabaseSelectionStrategy
    {
        private final ServerId serverId;

        StubStrategy( ServerId serverId )
        {
            super( "StubStrategy" );
            this.serverId = serverId;
        }

        @Override
        public Optional<ServerId> upstreamServerForDatabase( NamedDatabaseId ignored )
        {
            return Optional.ofNullable( serverId );
        }
    }
}
