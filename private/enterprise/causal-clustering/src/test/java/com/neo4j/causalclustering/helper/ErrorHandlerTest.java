/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ErrorHandlerTest
{
    private static final String FAILMESSAGE = "More fail";

    @Test
    public void shouldExecuteAllFailingOperations()
    {
        AtomicBoolean bool = new AtomicBoolean( false );
        try
        {
            ErrorHandler.runAll( "test", Assert::fail, () ->
            {
                bool.set( true );
                throw new IllegalStateException( FAILMESSAGE );
            } );
            fail();
        }
        catch ( RuntimeException e )
        {
            assertEquals( "test", e.getMessage() );
            Throwable cause = e.getCause();
            assertEquals( AssertionError.class, cause.getClass() );
            Throwable[] suppressed = e.getSuppressed();
            assertEquals( 1, suppressed.length );
            assertEquals( IllegalStateException.class, suppressed[0].getClass() );
            assertEquals( "More fail", suppressed[0].getMessage() );
            assertTrue( bool.get() );
        }
    }
}
