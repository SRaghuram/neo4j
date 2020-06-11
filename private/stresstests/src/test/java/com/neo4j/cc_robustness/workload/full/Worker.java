/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload.full;

import com.neo4j.cc_robustness.CcInstance;
import com.neo4j.cc_robustness.Orchestrator;
import com.neo4j.cc_robustness.util.Duration;
import com.neo4j.cc_robustness.workload.InstanceSelector;

import java.rmi.RemoteException;
import java.util.Random;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.cc_robustness.util.TimeUtil.sleepQuietly;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Worker extends Thread
{
    protected final Random random = new Random();
    protected final Orchestrator orchestrator;
    protected final Log log;
    final Reporting reporting;
    private final String id;
    private final Configuration config;
    private final Condition endCondition;
    private volatile int currentServerId = -1;
    private volatile boolean paused;
    private volatile boolean waterBucket; // for waking it up if sleeping
    Worker( String id, Configuration config, Orchestrator orchestrator, Condition endCondition, RemoteControl remoteControl, ReportingFactory reporting,
            LogProvider logProvider )
    {
        super( "CC Worker " + id );
        this.log = logProvider.getLog( getClass() );
        this.id = id;
        this.config = config;
        this.orchestrator = orchestrator;
        this.endCondition = endCondition;
        this.reporting = reporting.create( "CC Worker " + id );
        remoteControl.addListener( new RemoteControlListener.Adapter()
        {
            @Override
            public void pausedChanged( boolean pause )
            {
                paused = pause;
            }
        } );
    }

    protected boolean paused()
    {
        return paused;
    }

    @Override
    public void run()
    {
        while ( !endCondition.met() )
        {
            sleeep( timeToSleepBefore() );

            if ( endCondition.met() || paused() )
            {
                continue;
            }

            try
            {
                doOneOperation();
            }
            catch ( Throwable e )
            {
                reporting.fatalError( "Unhandled error", e );
            }
        }
        reporting.done();
        log.info( this + " exiting" );
    }

    protected String state()
    {
        return id + getState().name().substring( 0, 1 ) + currentServerId;
    }

    void sleeep( long time )
    {
        long sleepEndTime = nanoTime() + MILLISECONDS.toNanos( time );
        while ( !endCondition.met() && (nanoTime() < sleepEndTime || paused()) && !waterBucket )
        {
            sleepQuietly( 10 );
        }
        waterBucket = false;
    }

    void wakeUp()
    {
        waterBucket = true;
    }

    private long timeToSleepBefore()
    {
        return plusMinus( config.waitBetweenOperations().to( MILLISECONDS ), config.waitBetweenOperations().variance() );
    }

    /**
     * Anything thrown out from this method is considered a fatal error.
     */
    protected void doOneOperation() throws Throwable
    {
        CcInstance instance = config.instanceSelector().select( orchestrator );
        if ( instance == null || !instance.isAvailable() )
        {
            return; // Temporarily shut down?
        }

        try
        {
            setCurrentServerId( instance.getServerId() );

            // Being nice and not battering it with queries if there's no master yet
            if ( config.gentleness() == Gentleness.very && orchestrator.getLeaderServerId() == -1 )
            {
                return;
            }

            doOperationUnderWatchfulEye( instance );
        }
        catch ( Throwable e )
        {
            try
            {
                reporting.failedTransaction( currentServerId, e );
            }
            finally
            {
                sleeep( config.waitAfterFailure().to( MILLISECONDS ) );
            }
        }
    }

    protected void doOperationUnderWatchfulEye( CcInstance instance ) throws RemoteException
    {
        boolean shouldDoBigOperation = config.bigTransactionProbability() > 0 && random.nextFloat() <= config.bigTransactionProbability();
        reporting.startedTransaction();
        instance.doBatchOfOperations( shouldDoBigOperation ? random.nextInt( 5000 ) + 1000 : (int) plusMinus( config.transactionSize(), 0.25f ) );
        reporting.succeededTransaction( currentServerId );
    }

    private long plusMinus( long value, double partOfValuePlusOrMinus )
    {
        return value + (long) (value * (partOfValuePlusOrMinus * (1 - random.nextDouble() * 2)));
    }

    void setCurrentServerId( int id )
    {
        this.currentServerId = id;
    }

    public interface Configuration
    {
        Duration waitBetweenOperations();

        Gentleness gentleness();

        float bigTransactionProbability();

        int transactionSize();

        Duration waitAfterFailure();

        InstanceSelector instanceSelector();
    }
}
