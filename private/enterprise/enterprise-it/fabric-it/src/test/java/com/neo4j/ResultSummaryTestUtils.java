/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ProfiledPlan;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ResultSummaryTestUtils
{
    public static ProfileStats stats( int numRecords, boolean hasDbHits, ProfileStats... children )
    {
        return new ProfileStats( numRecords, hasDbHits, Arrays.asList( children ) );
    }

    public static ExpectedPlan plan( String operatorType, ExpectedPlan... children )
    {
        return new ExpectedPlan( operatorType, Arrays.asList( children ) );
    }

    public static class ProfileStats
    {
        private final int numRecords;
        private final boolean hasDbHits;
        private final List<ProfileStats> children;

        ProfileStats( int numRecords, boolean hasDbHits, List<ProfileStats> children )
        {
            this.numRecords = numRecords;
            this.hasDbHits = hasDbHits;
            this.children = children;
        }

        void assertStats( ProfiledPlan profiledPlan )
        {
            assertEquals( numRecords, profiledPlan.records() );
            assertEquals( hasDbHits, profiledPlan.dbHits() > 0 );

            assertEquals( children.size(), profiledPlan.children().size() );

            IntStream.range( 0, children.size() ).forEach( i -> children.get( i ).assertStats( profiledPlan.children().get( i ) ) );
        }
    }

    public static class ExpectedPlan
    {

        private final String operatorType;
        private final List<ExpectedPlan> children;

        ExpectedPlan( String operatorType, List<ExpectedPlan> children )
        {
            this.operatorType = operatorType;
            this.children = children;
        }

        public void assertPlan( Plan driverPlan )
        {
            assertEquals( operatorType, driverPlan.operatorType() );
            assertEquals( children.size(), driverPlan.children().size() );

            IntStream.range( 0, children.size() ).forEach( i -> children.get( i ).assertPlan( driverPlan.children().get( i ) ) );
        }
    }
}
