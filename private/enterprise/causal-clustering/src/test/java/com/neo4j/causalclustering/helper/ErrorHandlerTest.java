/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorHandlerTest
{
    private static final String FAILMESSAGE = "More fail";

    @Test
    void shouldExecuteAllFailingOperations()
    {
        AtomicBoolean bool = new AtomicBoolean( false );
        try
        {
            ErrorHandler.runAll( "test", () ->
            {
                throw new Exception();
            }, () ->
            {
                bool.set( true );
                throw new IllegalStateException( FAILMESSAGE );
            } );
        }
        catch ( RuntimeException e )
        {
            assertThat( e ).hasMessage( "test" ).isInstanceOf( RuntimeException.class );
            Throwable cause = e.getCause();
            assertThat( cause ).isInstanceOf( Exception.class );
            Throwable[] suppressed = e.getSuppressed();
            assertThat( suppressed ).hasSize( 1 );
            assertThat( suppressed[0] ).isInstanceOf( IllegalStateException.class ).hasMessage( FAILMESSAGE );
            assertThat( bool ).isTrue();
        }
    }
}
