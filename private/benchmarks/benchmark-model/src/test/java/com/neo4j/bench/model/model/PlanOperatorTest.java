/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class PlanOperatorTest
{
    @Test
    public void shouldDoEqualityAndEquivalence()
    {
        PlanOperator plan = new PlanOperator( 1, "a", 1L, 1L, 1L );
        PlanOperator planCopy = new PlanOperator( 1, "a", 1L, 1L, 1L );
        PlanOperator planEquivalent = new PlanOperator( 1, "a", 1L, 2L, 2L );
        PlanOperator planDiffId = new PlanOperator( 2, "a", 1L, 1L, 1L );
        PlanOperator planDiffName = new PlanOperator( 1, "b", 1L, 1L, 1L );
        PlanOperator planDiffEstRows = new PlanOperator( 1, "a", 2L, 1L, 1L );

        assertThat( plan, equalTo( planCopy ) );
        assertThat( plan, not( equalTo( planEquivalent ) ) );
        assertThat( plan, not( equalTo( planDiffId ) ) );
        assertThat( plan, not( equalTo( planDiffName ) ) );
        assertThat( plan, not( equalTo( planDiffEstRows ) ) );

        assertThat( plan, isEquivalent( planCopy ) );
        assertThat( plan, isEquivalent( planEquivalent ) );
        assertThat( plan, not( isEquivalent( planDiffId ) ) );
        assertThat( plan, not( isEquivalent( planDiffName ) ) );
        assertThat( plan, not( isEquivalent( planDiffEstRows ) ) );
    }

    @Test
    public void shouldDoEqualityAndEquivalenceWithChildren()
    {
        PlanOperator plan = new PlanOperator( 1, "a", 1L, 1L, 1L );
        plan.addChild( new PlanOperator( 2, "b", 1L, 1L, 1L ) );
        plan.addChild( new PlanOperator( 3, "c", 1L, 1L, 1L ) );

        PlanOperator planCopy = new PlanOperator( 1, "a", 1L, 1L, 1L );
        planCopy.addChild( new PlanOperator( 2, "b", 1L, 1L, 1L ) );
        planCopy.addChild( new PlanOperator( 3, "c", 1L, 1L, 1L ) );

        PlanOperator planEquivalent = new PlanOperator( 1, "a", 1L, 2L, 2L );
        planEquivalent.addChild( new PlanOperator( 2, "b", 1L, 2L, 2L ) );
        planEquivalent.addChild( new PlanOperator( 3, "c", 1L, 2L, 2L ) );

        PlanOperator planDiffChildId = new PlanOperator( 1, "a", 1L, 2L, 2L );
        planDiffChildId.addChild( new PlanOperator( 2, "b", 1L, 2L, 2L ) );
        planDiffChildId.addChild( new PlanOperator( 4, "c", 1L, 2L, 2L ) );

        PlanOperator planOneFewerChildren = new PlanOperator( 1, "a", 1L, 1L, 1L );
        planOneFewerChildren.addChild( new PlanOperator( 2, "b", 1L, 1L, 1L ) );

        PlanOperator planOneMoreChildren = new PlanOperator( 1, "a", 1L, 1L, 1L );
        PlanOperator lhsChild = new PlanOperator( 2, "b", 1L, 1L, 1L );
        planOneMoreChildren.addChild( lhsChild );
        planOneMoreChildren.addChild( new PlanOperator( 3, "c", 1L, 1L, 1L ) );
        lhsChild.addChild( new PlanOperator( 4, "c", 1L, 1L, 1L ) );

        assertThat( plan, equalTo( planCopy ) );
        assertThat( plan, not( equalTo( planEquivalent ) ) );
        assertThat( plan, not( equalTo( planDiffChildId ) ) );
        assertThat( plan, not( equalTo( planOneFewerChildren ) ) );
        assertThat( plan, not( equalTo( planOneMoreChildren ) ) );

        assertThat( plan, isEquivalent( planCopy ) );
        assertThat( plan, isEquivalent( planEquivalent ) );
        assertThat( plan, not( isEquivalent( planDiffChildId ) ) );
        assertThat( plan, not( isEquivalent( planOneFewerChildren ) ) );
        assertThat( plan, not( isEquivalent( planOneMoreChildren ) ) );
    }

    @Test
    public void shouldDoEqualityAndEquivalenceOnLargePlan()
    {
        PlanOperator plan = new PlanOperator( 1, "a", 1L, 1L, 1L );
        PlanOperator lhsChild1 = new PlanOperator( 2, "b", 1L, 1L, 1L );
        plan.addChild( lhsChild1 );
        plan.addChild( new PlanOperator( 3, "c", 1L, 1L, 1L ) );
        PlanOperator lhsChild2 = new PlanOperator( 4, "b", 1L, 1L, 1L );
        lhsChild1.addChild( lhsChild2 );
        lhsChild1.addChild( new PlanOperator( 5, "b", 1L, 1L, 1L ) );
        PlanOperator lhsChild3 = new PlanOperator( 6, "b", 1L, 1L, 1L );
        lhsChild2.addChild( lhsChild3 );
        lhsChild2.addChild( new PlanOperator( 7, "b", 1L, 1L, 1L ) );

        PlanOperator planCopy = new PlanOperator( 1, "a", 1L, 1L, 1L );
        PlanOperator lhsChild1Copy = new PlanOperator( 2, "b", 1L, 1L, 1L );
        planCopy.addChild( lhsChild1Copy );
        planCopy.addChild( new PlanOperator( 3, "c", 1L, 1L, 1L ) );
        PlanOperator lhsChild2Copy = new PlanOperator( 4, "b", 1L, 1L, 1L );
        lhsChild1Copy.addChild( lhsChild2Copy );
        lhsChild1Copy.addChild( new PlanOperator( 5, "b", 1L, 1L, 1L ) );
        PlanOperator lhsChild3Copy = new PlanOperator( 6, "b", 1L, 1L, 1L );
        lhsChild2Copy.addChild( lhsChild3Copy );
        lhsChild2Copy.addChild( new PlanOperator( 7, "b", 1L, 1L, 1L ) );

        PlanOperator planEquivalent = new PlanOperator( 1, "a", 1L, 1L, 1L );
        PlanOperator lhsChild1Equivalent = new PlanOperator( 2, "b", 1L, 1L, 1L );
        planEquivalent.addChild( lhsChild1Equivalent );
        planEquivalent.addChild( new PlanOperator( 3, "c", 1L, 1L, 1L ) );
        PlanOperator lhsChild2Equivalent = new PlanOperator( 4, "b", 1L, 1L, 1L );
        lhsChild1Equivalent.addChild( lhsChild2Equivalent );
        lhsChild1Equivalent.addChild( new PlanOperator( 5, "b", 1L, 1L, 1L ) );
        PlanOperator lhsChild3Equivalent = new PlanOperator( 6, "b", 1L, 1L, 1L );
        lhsChild2Equivalent.addChild( lhsChild3Equivalent );
        lhsChild2Equivalent.addChild( new PlanOperator( 7, "b", 1L, 1L, 2L /*different*/ ) );

        PlanOperator planDifferent = new PlanOperator( 1, "a", 1L, 1L, 1L );
        PlanOperator lhsChild1Different = new PlanOperator( 2, "b", 1L, 1L, 1L );
        planDifferent.addChild( lhsChild1Different );
        planDifferent.addChild( new PlanOperator( 3, "c", 1L, 1L, 1L ) );
        PlanOperator lhsChild2Different = new PlanOperator( 4, "b", 1L, 1L, 1L );
        lhsChild1Different.addChild( lhsChild2Different );
        lhsChild1Different.addChild( new PlanOperator( 5, "b", 1L, 1L, 1L ) );
        PlanOperator lhsChild3Different = new PlanOperator( 6, "b", 1L, 1L, 1L );
        lhsChild2Different.addChild( lhsChild3Different );
        lhsChild2Different.addChild( new PlanOperator( 7, "b", 2L /*different*/, 1L, 2L /*different*/ ) );

        assertThat( plan, equalTo( planCopy ) );
        assertThat( plan, not( equalTo( planEquivalent ) ) );
        assertThat( plan, not( equalTo( planDifferent ) ) );

        assertThat( plan, isEquivalent( planCopy ) );
        assertThat( plan, isEquivalent( planEquivalent ) );
        assertThat( plan, not( isEquivalent( planDifferent ) ) );
    }

    @Test
    public void shouldSupportUpdatingProfiledCounts()
    {
        PlanOperator plan1 = new PlanOperator( 1, "a", 1L, 1L, 1L );
        PlanOperator lhsChild1 = new PlanOperator( 2, "b", 1L, 1L, 1L );
        plan1.addChild( lhsChild1 );
        plan1.addChild( new PlanOperator( 3, "c", 1L, 1L, 1L ) );
        PlanOperator lhsChild2 = new PlanOperator( 4, "b", 1L, 1L, 1L );
        lhsChild1.addChild( lhsChild2 );
        lhsChild1.addChild( new PlanOperator( 5, "b", 1L, 1L, 1L ) );
        PlanOperator lhsChild3 = new PlanOperator( 6, "b", 1L, 1L, 1L );
        lhsChild2.addChild( lhsChild3 );
        lhsChild2.addChild( new PlanOperator( 7, "b", 1L, 1L, 1L ) );

        PlanOperator plan2 = new PlanOperator( 1, "a", 1L, 1L, 1L );
        PlanOperator plan2LhsChild1 = new PlanOperator( 2, "b", 1L, 1L, 1L );
        plan2.addChild( plan2LhsChild1 );
        plan2.addChild( new PlanOperator( 3, "c", 1L, 1L, 1L ) );
        PlanOperator plan2LhsChild2 = new PlanOperator( 4, "b", 1L, 1L, 1L );
        plan2LhsChild1.addChild( plan2LhsChild2 );
        plan2LhsChild1.addChild( new PlanOperator( 5, "b", 1L, 1L, 1L ) );
        PlanOperator plan2LhsChild3 = new PlanOperator( 6, "b", 1L, 1L, 1L );
        plan2LhsChild2.addChild( plan2LhsChild3 );
        plan2LhsChild2.addChild( new PlanOperator( 7, "b", 1L, 1L, 1L ) );

        assertThat( plan1, equalTo( plan2 ) );

        // When plans are added, all operator counts should be incremented
        plan1.addProfiledCountsFrom( plan2 );

        PlanOperator plan3 = new PlanOperator( 1, "a", 1L, 2L, 2L );
        PlanOperator plan3LhsChild1 = new PlanOperator( 2, "b", 1L, 2L, 2L );
        plan3.addChild( plan3LhsChild1 );
        plan3.addChild( new PlanOperator( 3, "c", 1L, 2L, 2L ) );
        PlanOperator plan3LhsChild2 = new PlanOperator( 4, "b", 1L, 2L, 2L );
        plan3LhsChild1.addChild( plan3LhsChild2 );
        plan3LhsChild1.addChild( new PlanOperator( 5, "b", 1L, 2L, 2L ) );
        PlanOperator plan3LhsChild3 = new PlanOperator( 6, "b", 1L, 2L, 2L );
        plan3LhsChild2.addChild( plan3LhsChild3 );
        plan3LhsChild2.addChild( new PlanOperator( 7, "b", 1L, 2L, 2L ) );

        assertThat( plan1, equalTo( plan3 ) );

        // When profiler counts of a plan are divided, all operator counts should be updated
        plan1.divideProfiledCountsBy( 2 );

        assertThat( plan1, equalTo( plan2 ) );
    }

    private static IsEquivalent isEquivalent( PlanOperator expectedValue )
    {
        return new IsEquivalent( expectedValue );
    }

    private static class IsEquivalent extends BaseMatcher<PlanOperator>
    {
        private final PlanOperator expectedValue;

        private IsEquivalent( PlanOperator expectedValue )
        {
            this.expectedValue = expectedValue;
        }

        @Override
        public boolean matches( Object actualValue )
        {
            return ((PlanOperator) actualValue).isEquivalent( expectedValue );
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendValue( expectedValue );
        }
    }
}
