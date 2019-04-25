/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OperatorConnectorTest
{
    private ReconcilingDatabaseOperator reconciler = mock( ReconcilingDatabaseOperator.class );
    private OperatorConnector connector = new OperatorConnector( reconciler );

    @Test
    void shouldInvokeWithRegisteredOperators()
    {
        // given
        Operator operatorA = mock( Operator.class );
        Operator operatorB = mock( Operator.class );

        connector.register( operatorA );
        connector.register( operatorB );

        // when
        connector.trigger();

        // then
        List<Operator> operatorList = List.of( operatorA, operatorB );
        verify( reconciler ).reconcile( operatorList );
    }
}
