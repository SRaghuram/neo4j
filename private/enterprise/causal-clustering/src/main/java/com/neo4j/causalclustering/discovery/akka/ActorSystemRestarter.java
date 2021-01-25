/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.configuration.CausalClusteringInternalSettings;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.internal.helpers.DefaultTimeoutStrategy.constant;
import static org.neo4j.internal.helpers.DefaultTimeoutStrategy.exponential;

public class ActorSystemRestarter
{
    private final TimeoutStrategy timeoutStrategy;
    private final int maxRestartAttempts;
    private volatile boolean healthy = true;
    private volatile Throwable lastException;

    public static ActorSystemRestarter forConfig( Config config )
    {
        TimeoutStrategy timeoutStrategy = exponential(
                config.get( CausalClusteringInternalSettings.akka_actor_system_restarter_initial_delay ).toMillis(),
                config.get( CausalClusteringInternalSettings.akka_actor_system_restarter_max_delay ).toMillis(),
                TimeUnit.MILLISECONDS
        );
        return new ActorSystemRestarter( timeoutStrategy, config.get( CausalClusteringInternalSettings.akka_actor_system_max_restart_attempts ) );
    }

    @VisibleForTesting
    public static ActorSystemRestarter forTest( int maxRetries )
    {
        return new ActorSystemRestarter( constant( 1, TimeUnit.MILLISECONDS ), maxRetries );
    }

    private ActorSystemRestarter( TimeoutStrategy timeoutStrategy, int maxRestartAttempts )
    {
        this.timeoutStrategy = timeoutStrategy;
        this.maxRestartAttempts = maxRestartAttempts;
    }

    public boolean isHealthy()
    {
        return healthy;
    }

    private boolean doRestart( Callable<Boolean> retry )
    {
        try
        {
            var result = retry.call();
            lastException = null;
            return result;
        }
        catch ( Throwable e )
        {
            lastException = e;
            return false;
        }
    }

    public synchronized void restart( String name, Callable<Boolean> retry ) throws RestartFailedException
    {
        int count = 0;
        TimeoutStrategy.Timeout timeout = timeoutStrategy.newTimeout();

        // Structuring the loop like this allows:
        //  - set maxRestartAttempts to 0 to disable akka restart and go straight to throwing an error.
        //  - set maxRestartAttempts to a negative number (e.g. -1) to retry forever.
        while ( count < maxRestartAttempts || maxRestartAttempts < 0 )
        {
            if ( doRestart( retry ) )
            {
                healthy = true;
                return;
            }
            else
            {
                healthy = false;
            }

            count++;

            try
            {
                Thread.sleep( timeout.getAndIncrement() );
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException( e );
            }
        }

        healthy = false;
        throw lastException != null ? new RestartFailedException( lastException )
                                    : new RestartFailedException( String.format( "Unable to restart %s successfully after %s attempts", name, count ) );
    }

    public static class RestartFailedException extends Exception
    {
        public RestartFailedException( Throwable cause )
        {
            super( cause );
        }

        public RestartFailedException( String message )
        {
            super( message );
        }
    }
}
