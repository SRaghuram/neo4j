/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OperatorConnectorTest
{
    private final DbmsReconciler reconciler = mock( DbmsReconciler.class );
    private final OperatorConnector connector = new OperatorConnector( reconciler );

    @Test
    void shouldInvokeWithRegisteredOperators()
    {
        // given
        var operatorA = new MutableDbmsOperator();
        var operatorB = new MutableDbmsOperator();
        var operators = Set.<DbmsOperator>of( operatorA, operatorB );

        connector.setOperators( operators );

        // when
        connector.trigger( ReconcilerRequest.simple() );

        // then
        verify( reconciler ).reconcile( operators, ReconcilerRequest.simple() );
    }

    @Test
    void shouldConnectAllRegisteredOperators()
    {
        // given
        var operatorA = new MutableDbmsOperator();
        var operatorB = new MutableDbmsOperator();
        var operatorC = new MutableDbmsOperator();
        var operators = new ArrayList<DbmsOperator>( List.of( operatorA, operatorB, operatorC ) );

        // when
        connector.setOperators( operators );

        // then
        for ( var  operator : operators )
        {
           assertThat( operator.connected() ).contains( connector );
        }

        // given
        var operatorD = new MutableDbmsOperator();
        var operatorE = new MutableDbmsOperator();
        var additionalOperators = List.of( operatorD, operatorE );
        operators.addAll( additionalOperators );

        // when
        connector.setOperators( operators );

        // then
        for ( var additionalOperator : additionalOperators )
        {
            assertThat( additionalOperator.connected() ).contains( connector );
        }
    }

    @Test
    void shouldDisconnectAllReplacedOperators()
    {
        // given
        var operatorA = new MutableDbmsOperator();
        var operatorB = new MutableDbmsOperator();
        var operatorC = new MutableDbmsOperator();
        var originalOperators = List.<DbmsOperator>of( operatorA, operatorB, operatorC );

        // when
        connector.setOperators( originalOperators );

        // then
        for ( var  operator : originalOperators )
        {
            assertThat( operator.connected() ).contains( connector );
        }

        // given
        var operatorD = new MutableDbmsOperator();
        var operatorE = new MutableDbmsOperator();
        var newOperators = List.<DbmsOperator>of( operatorC, operatorD, operatorE );
        var replacedOperators = List.<DbmsOperator>of( operatorA, operatorB );

        // when
        connector.setOperators( newOperators );

        // then
        for ( var replacedOperator : replacedOperators )
        {
            assertThat( replacedOperator.connected() ).isEmpty();
        }
        assertThat( operatorC.connected() ).contains( connector );
    }
}
