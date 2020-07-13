/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.helper.Workload;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.CappedLogger;
import org.neo4j.values.storable.RandomValues;

class CreateNodesWithProperties extends Workload
{
    private static final Class[] TRANSIENT_EXCEPTIONS = new Class[]{
            DatabaseNotFoundException.class,
            DatabaseShutdownException.class,
            TimeoutException.class,
            TransactionFailureException.class
    };

    private static final Label label = Label.label( "Label" );

    private final Cluster cluster;
    private final CappedLogger txLogger;
    private final boolean enableIndexes;

    private long txSuccessCount;
    private long txFailCount;

    CreateNodesWithProperties( Control control, Resources resources, Config config )
    {
        super( control );
        this.enableIndexes = config.enableIndexes();
        this.cluster = resources.cluster();
        Log log = resources.logProvider().getLog( getClass() );
        this.txLogger = new CappedLogger( log, 5, TimeUnit.SECONDS, resources.clock() );
    }

    @Override
    public void prepare()
    {
        if ( enableIndexes )
        {
            setupIndexes( cluster );
        }
    }

    @Override
    protected void doWork()
    {
        txLogger.info( "SuccessCount: " + txSuccessCount + " FailCount: " + txFailCount );
        RandomValues randomValues = RandomValues.create();

        try
        {
            cluster.coreTx( ( db, tx ) ->
            {
                Node node = tx.createNode( label );
                for ( int i = 1; i <= 8; i++ )
                {
                    node.setProperty( prop( i ), randomValues.nextValue().asObject() );
                }
                tx.commit();
            } );
        }
        catch ( Throwable e )
        {
            txFailCount++;

            if ( isInterrupted( e ) || isTransient( e ) )
            {
                // whatever let's go on with the workload
                return;
            }

            throw new RuntimeException( e );
        }

        txSuccessCount++;
    }

    private static void setupIndexes( Cluster cluster )
    {
        try
        {
            cluster.coreTx( ( db, tx ) ->
            {
                for ( int i = 1; i <= 8; i++ )
                {
                    tx.schema().indexFor( label ).on( prop( i ) ).create();
                }
                tx.commit();
            } );
        }
        catch ( Throwable t )
        {
            throw new RuntimeException( t );
        }
    }

    private static String prop( int i )
    {
        return "prop" + i;
    }

    private boolean isTransient( Throwable e )
    {
        boolean isTransient = Stream.of( TRANSIENT_EXCEPTIONS ).anyMatch( cls -> cls.isInstance( e ) );

        return isTransient || isInterrupted( e.getCause() );
    }

    private boolean isInterrupted( Throwable e )
    {
        if ( e == null )
        {
            return false;
        }

        if ( e instanceof InterruptedException )
        {
            Thread.interrupted();
            return true;
        }

        return isInterrupted( e.getCause() );
    }
}
