/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream;

import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import org.neo4j.annotations.service.ServiceProvider;
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
        var memberId = new MemberId( UUID.randomUUID() );
        var populatedStrategy = new StubStrategy( memberId );
        var emptyStrategy = new StubStrategy( null );

        // when
        var nonEmptyMemberIds = populatedStrategy.upstreamMembersForDatabase( null ); // databaseId ignored
        var emptyMemberIds = emptyStrategy.upstreamMembersForDatabase( null );

        // then
        assertThat( nonEmptyMemberIds, hasSize( 1 ) );
        assertThat( nonEmptyMemberIds, contains( memberId ) );
        assertThat( emptyMemberIds, empty() );
    }

    private static class StubStrategy extends UpstreamDatabaseSelectionStrategy
    {
        private final MemberId memberId;

        StubStrategy( MemberId memberId )
        {
            super( "StubStrategy" );
            this.memberId = memberId;
        }

        @Override
        public Optional<MemberId> upstreamMemberForDatabase( NamedDatabaseId ignored )
        {
            return Optional.ofNullable( memberId );
        }
    }
}
