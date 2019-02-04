/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.internal;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.neo4j.helpers.collection.CollectorsUtil;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.monitoring.DatabasePanicEventGenerator;
import org.neo4j.monitoring.SingleDatabaseHealth;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class CompositeDatabaseHealthTest
{

    @Test
    void shouldPanicAllDatabasesTogether()
    {
        // given
        Map<String,SingleDatabaseHealth> dbHealths = Stream.generate( this::mockDBHealth ).limit( 5 ).collect( CollectorsUtil.pairsToMap() );
        CompositeDatabaseHealth compositeDatabaseHealth = new CompositeDatabaseHealth( dbHealths );

        // when
        Throwable expectedCause = new Exception( "Everybody panic!" );
        compositeDatabaseHealth.panic( expectedCause );

        // then
        for ( SingleDatabaseHealth dbHealth : dbHealths.values() )
        {
            assertFalse( dbHealth.isHealthy() );
            assertEquals( expectedCause, dbHealth.cause() );
        }
    }

    @Test
    void shouldAssertHealthyOnAllDatabasesTogether()
    {
        // given
        Map<String,SingleDatabaseHealth> dbHealths = Stream.generate( this::mockDBHealth ).limit( 5 ).collect( CollectorsUtil.pairsToMap() );
        CompositeDatabaseHealth compositeDatabaseHealth = new CompositeDatabaseHealth( dbHealths );

        // when
        compositeDatabaseHealth.assertHealthy( IllegalStateException.class );

        // then
        for ( SingleDatabaseHealth dbHealth : dbHealths.values() )
        {
            verify( dbHealth ).assertHealthy( eq( IllegalStateException.class ) );
        }
    }

    @Test
    void shouldSuppressMultipleCauses()
    {
        // given
        int numUnhealthyDBs = 5;
        Map<String,SingleDatabaseHealth> unhealthyDBs = Stream.generate( this::mockDBHealth )
                .peek( dbHealth -> dbHealth.other().panic( new Exception( "Error" ) ) )
                .limit( numUnhealthyDBs ).collect( CollectorsUtil.pairsToMap() );
        Pair<String,SingleDatabaseHealth> healthyDB = mockDBHealth();
        unhealthyDBs.put( healthyDB.first(), healthyDB.other() );
        CompositeDatabaseHealth compositeDatabaseHealth = new CompositeDatabaseHealth( unhealthyDBs );

        // then
        assertFalse( compositeDatabaseHealth.isHealthy() );
        Throwable compositeCause = compositeDatabaseHealth.cause();
        assertThat( compositeCause.getMessage(), containsString( "Some of the databases have panicked" ) );
        Throwable[] suppressed = compositeCause.getSuppressed();
        assertEquals( suppressed.length, numUnhealthyDBs );
    }

    private Pair<String,SingleDatabaseHealth> mockDBHealth()
    {

        DatabasePanicEventGenerator generator = mock( DatabasePanicEventGenerator.class );
        return Pair.of( UUID.randomUUID().toString(),
                spy( new SingleDatabaseHealth( generator, NullLogProvider.getInstance().getLog( SingleDatabaseHealth.class ) ) ) );
    }

}
