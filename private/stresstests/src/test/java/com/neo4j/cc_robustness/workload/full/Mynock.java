/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload.full;

import com.neo4j.cc_robustness.CcInstance;
import com.neo4j.cc_robustness.Orchestrator;
import com.neo4j.cc_robustness.Restoration;
import com.neo4j.cc_robustness.util.Duration;

import java.rmi.RemoteException;

import org.neo4j.logging.LogProvider;

import static com.neo4j.cc_robustness.CcInstance.NF_IN;
import static com.neo4j.cc_robustness.CcInstance.NF_OUT;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Chewing on the network cables.
 * <p>
 * See http://starwars.wikia.com/wiki/Mynock
 */
public class Mynock extends Worker
{
    private final Configuration config;

    Mynock( Configuration config, Orchestrator orchestrator, Condition endCondition, RemoteControl remoteControl, ReportingFactory reporting,
            LogProvider logging )
    {
        super( "Mynock", config, orchestrator, endCondition, remoteControl, reporting, logging );
        this.config = config;
    }

    @Override
    protected void doOperationUnderWatchfulEye( CcInstance instance ) throws RemoteException
    {
        // Randomize network outage out/in
        int flags = (random.nextBoolean() ? NF_OUT : 0) | (random.nextBoolean() ? NF_IN : 0);
        flags = flags == 0 ? (NF_OUT | NF_IN) : flags;

        // Chew those cables
        Restoration restore = orchestrator.getInstance( instance.getServerId() ).blockNetwork( flags );
        if ( restore == null )
        {
            return;
        }
        long outageTime = config.cableOutageMaxTime().to( SECONDS, random );
        log.info( "%s: chewing on the %s cables of %d for %d seconds.", getName(), humanReadable( flags ), instance.getServerId(), outageTime );
        try
        {   // Wait random time
            sleeep( SECONDS.toMillis( outageTime ) );
        }
        finally
        {   // Put it back again
            restore.restore();
            log.info( getName() + ": restored cables " + humanReadable( flags ) + " on " + instance.getServerId() );
        }
    }

    private String humanReadable( int flags )
    {
        StringBuilder builder = new StringBuilder();
        if ( (flags & NF_OUT) != 0 )
        {
            builder.append( "outgoing" );
        }
        if ( (flags & NF_IN) != 0 )
        {
            if ( builder.length() > 0 )
            {
                builder.append( " and " );
            }
            builder.append( "incoming" );
        }
        return builder.toString();
    }

    public interface Configuration extends Worker.Configuration
    {
        Duration cableOutageMaxTime();
    }
}
