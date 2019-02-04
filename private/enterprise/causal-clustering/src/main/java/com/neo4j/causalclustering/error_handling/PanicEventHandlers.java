/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import com.neo4j.causalclustering.net.Server;

import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityRequirement;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.monitoring.DatabaseHealth;

import static com.neo4j.causalclustering.error_handling.PanicException.EXCEPTION;

public final class PanicEventHandlers
{
    private PanicEventHandlers()
    {
    }

    public static DbHealthPanicEventHandler dbHealthEventHandler( DatabaseHealth internalDatabasesHealth )
    {
        return new DbHealthPanicEventHandler( internalDatabasesHealth );
    }

    public static RaiseAvailabilityGuardEventHandler raiseAvailabilityGuardEventHandler( AvailabilityGuard availabilityGuard )
    {
        return new RaiseAvailabilityGuardEventHandler( availabilityGuard );
    }

    public static DisableServerEventHandler disableServerEventHandler( Server server )
    {
        return new DisableServerEventHandler( server );
    }

    public static ShutdownLifeCycle shutdownLifeCycle( LifeSupport life )
    {
        return new ShutdownLifeCycle( life );
    }

    private static class DbHealthPanicEventHandler implements PanicEventHandler
    {
        private final DatabaseHealth internalDatabasesHealth;

        private DbHealthPanicEventHandler( DatabaseHealth internalDatabasesHealth )
        {
            this.internalDatabasesHealth = internalDatabasesHealth;
        }

        @Override
        public void onPanic()
        {
            internalDatabasesHealth.panic( EXCEPTION );
        }
    }

    private static class RaiseAvailabilityGuardEventHandler implements PanicEventHandler
    {
        private final AvailabilityGuard availabilityGuard;
        AvailabilityRequirement panickedRequirement = () -> "System has panicked and need to be restarted.";

        private RaiseAvailabilityGuardEventHandler( AvailabilityGuard availabilityGuard )
        {
            this.availabilityGuard = availabilityGuard;
        }

        @Override
        public void onPanic()
        {
            availabilityGuard.require( panickedRequirement );
        }
    }

    private static class DisableServerEventHandler implements PanicEventHandler
    {

        private final Server server;

        private DisableServerEventHandler( Server server )
        {
            this.server = server;
        }

        @Override
        public void onPanic()
        {
            try
            {
                server.disable();
            }
            catch ( Throwable throwable )
            {
                // no-op - best effort.
            }
        }
    }

    private static class ShutdownLifeCycle implements PanicEventHandler
    {
        private final Lifecycle lifecycle;

        ShutdownLifeCycle( Lifecycle lifecycle )
        {
            this.lifecycle = lifecycle;
        }

        @Override
        public void onPanic()
        {
            try
            {
                lifecycle.shutdown();
            }
            catch ( Throwable ignore )
            {
                // best effort
            }
        }
    }
}
