/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;

class DatabasePenaltiesTest
{
    private final FakeClock fakeClock = Clocks.fakeClock();
    private final long suspensionTime = 10;
    private final TimeUnit timeUnit = TimeUnit.MILLISECONDS;
    private final MemberId member1 = new MemberId( UUID.randomUUID() );
    private final MemberId member2 = new MemberId( UUID.randomUUID() );
    private final NamedDatabaseId db1 = from( "db-one", UUID.randomUUID() );
    private final NamedDatabaseId db2 = from( "db-two", UUID.randomUUID() );
    private final DatabasePenalties lts = new DatabasePenalties( suspensionTime, timeUnit, fakeClock );

    @Test
    void shouldSuspendDatabasesForMember()
    {
        lts.issuePenalty( member1, db2 );
        lts.issuePenalty( member1, db1 );
        lts.issuePenalty( member2, db2 );

        assertIsSuspended( lts.suspendedDatabases( member1 ), db1, db2 );
        assertIsSuspended( lts.suspendedDatabases( member2 ), db2 );
        assertFalse( lts.notSuspended( db1.databaseId(), member1 ) );
        assertFalse( lts.notSuspended( db2.databaseId(), member1 ) );
        assertTrue( lts.notSuspended( db1.databaseId(), member2 ) );
        assertFalse( lts.notSuspended( db2.databaseId(), member2 ) );
    }

    @Test
    void shouldSuspendAndUnsuspendDatabasesForMember()
    {
        lts.issuePenalty( member1, db1 );
        lts.issuePenalty( member1, db2 );
        lts.issuePenalty( member2, db2 );

        fakeClock.forward( suspensionTime, timeUnit );

        assertIsSuspended( lts.suspendedDatabases( member1 ), db1, db2 );
        assertIsSuspended( lts.suspendedDatabases( member2 ), db2 );

        fakeClock.forward( 1, timeUnit );

        assertIsSuspended( lts.suspendedDatabases( member1 ) );
        assertIsSuspended( lts.suspendedDatabases( member2 ) );
    }

    @Test
    void shouldUpdateSuspension()
    {
        lts.issuePenalty( member1, db2 );
        lts.issuePenalty( member1, db1 );
        lts.issuePenalty( member2, db2 );

        fakeClock.forward( suspensionTime, timeUnit );
        lts.issuePenalty( member1, db2 );

        assertIsSuspended( lts.suspendedDatabases( member1 ), db1, db2 );
        assertIsSuspended( lts.suspendedDatabases( member2 ), db2 );

        fakeClock.forward( 1, timeUnit );

        assertIsSuspended( lts.suspendedDatabases( member1 ), db2 );
        assertIsSuspended( lts.suspendedDatabases( member2 ) );

        fakeClock.forward( suspensionTime, timeUnit );
        assertIsSuspended( lts.suspendedDatabases( member1 ) );
    }

    private void assertIsSuspended( Set<NamedDatabaseId> actual, NamedDatabaseId... expected )
    {
        assertThat( actual ).containsExactlyInAnyOrder( expected );
    }
}
