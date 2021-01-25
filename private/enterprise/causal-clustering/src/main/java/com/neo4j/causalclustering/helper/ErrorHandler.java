/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper;

import java.util.ArrayList;
import java.util.List;

public class ErrorHandler implements AutoCloseable
{
    private final List<Throwable> throwables = new ArrayList<>();
    private final String message;

    /**
     * Ensures each action is executed. Any throwables will be saved and thrown after all actions have been executed. The first caught throwable will be cause
     * and any other will be added as suppressed.
     *
     * @param description The exception message if any are thrown.
     * @param actions Throwing runnables to execute.
     * @throws RuntimeException thrown if any action throws after all have been executed.
     */
    public static void runAll( String description, ThrowingRunnable... actions ) throws RuntimeException
    {
        try ( ErrorHandler errorHandler = new ErrorHandler( description ) )
        {
            for ( ThrowingRunnable action : actions )
            {
                errorHandler.execute( action );
            }
        }
    }

    public ErrorHandler( String message )
    {
        this.message = message;
    }

    public void execute( ThrowingRunnable throwingRunnable )
    {
        try
        {
            throwingRunnable.run();
        }
        catch ( Throwable e )
        {
            throwables.add( e );
        }
    }

    public void add( Throwable throwable )
    {
        throwables.add( throwable );
    }

    @Override
    public void close() throws RuntimeException
    {
        throwIfException();
    }

    private void throwIfException()
    {
        RuntimeException runtimeException = null;
        for ( Throwable throwable : throwables )
        {
            if ( runtimeException == null )
            {
                runtimeException = new RuntimeException( message, throwable );
            }
            else
            {
                runtimeException.addSuppressed( throwable );
            }
        }
        if ( runtimeException != null )
        {
            throw runtimeException;
        }
    }

    public interface ThrowingRunnable
    {
        void run() throws Throwable;
    }
}
