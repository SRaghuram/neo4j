/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.fabric.executor.FabricSecondaryException;
import org.neo4j.fabric.executor.Location;
import org.neo4j.values.virtual.MapValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FabricDriverRxTransactionTest
{
    @Test
    void testSecondaryError()
    {
        var rxTransaction = mock( RxTransaction.class );
        var rxSession = mock( RxSession.class );
        var location = mock( Location.Remote.class );

        var firstRxResult = mockRxResult( new DatabaseException( "", "Primary Error" ) );
        var secondRxResult = mockRxResult( new DatabaseException( "", "Secondary Error" ) );
        when( rxTransaction.run( any(), anyMap() ) ).thenReturn( firstRxResult, secondRxResult );

        var fabricDriverTransaction = new FabricDriverRxTransaction( rxTransaction, rxSession, location );
        var result1 = fabricDriverTransaction.run( "", MapValue.EMPTY );
        var result2 = fabricDriverTransaction.run( "", MapValue.EMPTY );

        var firstException = assertThrows( FabricException.class, () -> result1.records().collectList().block() );
        assertEquals( firstException.getMessage(), "Remote execution failed with code  and message 'Primary Error'" );
        var secondException = assertThrows( FabricSecondaryException.class, () -> result2.records().collectList().block() );
        assertEquals( secondException.getMessage(), "Remote execution failed with code  and message 'Secondary Error'" );
        assertEquals( secondException.getPrimaryException().getMessage(), "Remote execution failed with code  and message 'Primary Error'" );
    }

    private RxResult mockRxResult( Exception e )
    {
        var result = mock( RxResult.class );
        when( result.keys() ).thenReturn( Mono.just( List.of( "a", "b" ) ) );
        when( result.records() ).thenReturn( Flux.error( e ) );
        when( result.consume() ).thenReturn( Mono.empty() );
        return result;
    }
}
