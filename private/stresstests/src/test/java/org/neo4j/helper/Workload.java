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

    public Workload( Control control )
    {
        this.control = control;
    }

    @Override
    public final void run()
    {
        try
        {
            while ( control.keepGoing() )
            {
                doWork();
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
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
