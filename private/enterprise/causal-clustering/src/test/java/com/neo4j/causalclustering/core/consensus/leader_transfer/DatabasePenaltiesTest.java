/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

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
    private final DatabasePenalties databasePenalties = new DatabasePenalties( suspensionTime, timeUnit, fakeClock );

    @Test
    void shouldSuspendDatabasesForMember()
    {
        databasePenalties.issuePenalty( member1, db2 );
        databasePenalties.issuePenalty( member1, db1 );
        databasePenalties.issuePenalty( member2, db2 );

        assertIsSuspended( databasePenalties, member1, db1, db2 );
        assertIsSuspended( databasePenalties, member2, db2 );
        assertNotSuspended( databasePenalties, member2, db1 );
    }

    @Test
    void shouldSuspendAndUnsuspendDatabasesForMember()
    {
        databasePenalties.issuePenalty( member1, db1 );
        databasePenalties.issuePenalty( member1, db2 );
        databasePenalties.issuePenalty( member2, db2 );

        fakeClock.forward( suspensionTime, timeUnit );

        assertIsSuspended( databasePenalties, member1, db1, db2 );
        assertIsSuspended( databasePenalties, member2, db2 );
        assertNotSuspended( databasePenalties, member2, db1 );

        fakeClock.forward( 1, timeUnit );

        assertNotSuspended( databasePenalties, member1, db1, db2 );
        assertNotSuspended( databasePenalties, member2, db1, db2 );
    }

    @Test
    void shouldUpdateSuspension()
    {
        databasePenalties.issuePenalty( member1, db2 );
        databasePenalties.issuePenalty( member1, db1 );
        databasePenalties.issuePenalty( member2, db2 );

        fakeClock.forward( suspensionTime, timeUnit );
        databasePenalties.issuePenalty( member1, db2 );

        assertIsSuspended( databasePenalties, member1, db1, db2 );
        assertIsSuspended( databasePenalties, member2, db2 );
        assertNotSuspended( databasePenalties, member2, db1 );

        fakeClock.forward( 1, timeUnit );

        assertIsSuspended( databasePenalties, member1, db2 );
        assertNotSuspended( databasePenalties, member1, db1 );
        assertNotSuspended( databasePenalties, member2, db1, db2 );

        fakeClock.forward( suspensionTime, timeUnit );
        assertNotSuspended( databasePenalties, member1, db1, db2 );
    }

    private static void assertIsSuspended( DatabasePenalties databasePenalties, MemberId memberId, NamedDatabaseId... expected )
    {
        for ( NamedDatabaseId namedDatabaseId : expected )
        {
            assertFalse( databasePenalties.notSuspended( namedDatabaseId.databaseId(), memberId ) );
        }
    }

    private static void assertNotSuspended( DatabasePenalties databasePenalties, MemberId memberId, NamedDatabaseId... expected )
    {
        for ( NamedDatabaseId namedDatabaseId : expected )
        {
            assertTrue( databasePenalties.notSuspended( namedDatabaseId.databaseId(), memberId ) );
        }
    }
}
