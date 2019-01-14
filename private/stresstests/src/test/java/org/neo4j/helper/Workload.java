/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.helper;

import org.neo4j.causalclustering.stresstests.Control;

public abstract class Workload implements Runnable
{
    protected final Control control;
    private final long sleepTimeMillis;

    public Workload( Control control )
    {
        this( control, 0 );
    }

    @SuppressWarnings( "WeakerAccess" )
    public Workload( Control control, long sleepTimeMillis )
    {
        this.control = control;
        this.sleepTimeMillis = sleepTimeMillis;
    }

    @Override
    public final void run()
    {
        try
        {
            while ( control.keepGoing() )
            {
                doWork();
                if ( sleepTimeMillis != 0 )
                {
                    Thread.sleep( sleepTimeMillis );
                }
            }
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
        }
        catch ( Throwable t )
        {
            control.onFailure( t );
        }
    }

    protected abstract void doWork() throws Exception;

    @SuppressWarnings( "RedundantThrows" )
    public void prepare() throws Exception
    {
    }

    @SuppressWarnings( "RedundantThrows" )
    public void validate() throws Exception
    {
    }
}
