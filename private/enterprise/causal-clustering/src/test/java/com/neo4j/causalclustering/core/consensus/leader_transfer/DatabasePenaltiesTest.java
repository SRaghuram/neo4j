/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;

class DatabasePenaltiesTest
{
    private final FakeClock fakeClock = Clocks.fakeClock();
    private final Duration suspensionTime = Duration.ofMillis( 10 );
    private final MemberId member1 = new MemberId( UUID.randomUUID() );
    private final MemberId member2 = new MemberId( UUID.randomUUID() );
    private final NamedDatabaseId db1 = from( "db-one", UUID.randomUUID() );
    private final NamedDatabaseId db2 = from( "db-two", UUID.randomUUID() );
    private final DatabasePenalties databasePenalties = new DatabasePenalties( suspensionTime, fakeClock );

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

        fakeClock.forward( suspensionTime.minusMillis( 1 ) );

        assertIsSuspended( databasePenalties, member1, db1, db2 );
        assertIsSuspended( databasePenalties, member2, db2 );
        assertNotSuspended( databasePenalties, member2, db1 );

        fakeClock.forward( Duration.ofMillis( 1 ) );

        assertNotSuspended( databasePenalties, member1, db1, db2 );
        assertNotSuspended( databasePenalties, member2, db1, db2 );
    }

    @Test
    void shouldUpdateSuspension()
    {
        databasePenalties.issuePenalty( member1, db2 );
        databasePenalties.issuePenalty( member1, db1 );
        databasePenalties.issuePenalty( member2, db2 );

        fakeClock.forward( suspensionTime.minusMillis( 1 ) );
        databasePenalties.issuePenalty( member1, db2 );

        assertIsSuspended( databasePenalties, member1, db1, db2 );
        assertIsSuspended( databasePenalties, member2, db2 );
        assertNotSuspended( databasePenalties, member2, db1 );

        fakeClock.forward( Duration.ofMillis( 1 ) );

        assertIsSuspended( databasePenalties, member1, db2 );
        assertNotSuspended( databasePenalties, member1, db1 );
        assertNotSuspended( databasePenalties, member2, db1, db2 );

        fakeClock.forward( suspensionTime );
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
