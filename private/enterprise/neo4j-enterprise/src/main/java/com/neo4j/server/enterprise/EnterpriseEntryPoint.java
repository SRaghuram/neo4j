/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.server.BlockingBootstrapper;
import org.neo4j.server.Bootstrapper;
import org.neo4j.server.NeoBootstrapper;
import org.neo4j.server.startup.EntryPoint;

import static org.neo4j.internal.unsafe.UnsafeUtil.disableIllegalAccessLogger;

@ServiceProvider
public class EnterpriseEntryPoint implements EntryPoint
{
    private static Bootstrapper bootstrapper;

    public static void main( String[] args )
    {
        disableIllegalAccessLogger();
        int status = NeoBootstrapper.start( new EnterpriseBootstrapper(), args );
        if ( status != 0 )
        {
            System.exit( status );
        }
    }

    /**
     * Used by the windows service wrapper
     */
    @SuppressWarnings( "unused" )
    public static void start( String[] args )
    {
        bootstrapper = new BlockingBootstrapper( new EnterpriseBootstrapper() );
        System.exit( NeoBootstrapper.start( bootstrapper, args ) );
    }

    /**
     * Used by the windows service wrapper
     */
    @SuppressWarnings( "unused" )
    public static void stop( @SuppressWarnings( "UnusedParameters" ) String[] args )
    {
        if ( bootstrapper != null )
        {
            bootstrapper.stop();
        }
    }

    @Override
    public Priority getPriority()
    {
        return Priority.MEDIUM;
    }
}
