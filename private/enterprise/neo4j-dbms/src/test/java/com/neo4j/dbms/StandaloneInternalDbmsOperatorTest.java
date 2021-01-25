/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StandaloneInternalDbmsOperatorTest
{
    @Test
    void shouldDesireStoppedForPanickedDatabase()
    {
        // given
        var reconciler = mock( DbmsReconciler.class );
        when( reconciler.reconcile( anyList(), any() ) ).thenReturn( ReconcilerResult.EMPTY );
        var operator = new StandaloneInternalDbmsOperator( NullLogProvider.getInstance() );
        var connector = new TestOperatorConnector( reconciler );
        operator.connect( connector );

        var fooDb = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var err = new Exception( "Cause of panic" );

        var expectedDesired = Map.of( "foo", new EnterpriseDatabaseState( fooDb, STOPPED ) );
        var expectedRequest = ReconcilerRequest.panickedTarget( fooDb, err ).build();
        var expectedTriggerCall = Pair.of( expectedDesired, expectedRequest );

        // when
        operator.stopOnPanic( fooDb, err );

        // then
        var triggerCalls = connector.triggerCalls();
        assertThat( "Operator should only trigger for single database", triggerCalls, hasSize( 1 ) );
        assertThat( "Operator should desire state STOPPED for foo", triggerCalls, hasItem( expectedTriggerCall ) );
    }

}
