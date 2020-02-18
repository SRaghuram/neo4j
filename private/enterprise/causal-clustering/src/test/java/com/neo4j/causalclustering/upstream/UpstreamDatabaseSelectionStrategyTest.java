/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
