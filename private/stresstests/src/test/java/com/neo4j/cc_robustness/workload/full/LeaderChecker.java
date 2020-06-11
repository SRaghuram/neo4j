/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload.full;

import com.neo4j.cc_robustness.CcInstance;
import com.neo4j.cc_robustness.Orchestrator;
import com.neo4j.cc_robustness.workload.GraphOperations.Operation;

import java.rmi.RemoteException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.logging.LogProvider;

/*
 * Makes sure that the master is responsive at all times, if not it produces a FATAL error
 */
class LeaderChecker extends Worker
{
    private final AtomicInteger consecutiveFailures = new AtomicInteger();
    private final Configuration config;

    LeaderChecker( Configuration config, Orchestrator orchestrator, Condition endCondition, RemoteControl remoteControl, ReportingFactory reporting,
            LogProvider logProvider )
    {
        super( "MasterWorker", config, orchestrator, endCondition, remoteControl, reporting, logProvider );
        this.config = config;
    }

    @Override
    protected void doOneOperation() throws RemoteException
    {
        // Do a simple write operation, on the leader
        // just to see if it's responding and is alright. If it isn't successful
        // consider it pretty much fatal (if we weren't just now taking down the
        // master or switching master or whatever).
        CcInstance leader = orchestrator.getLeaderInstance();
        if ( leader == null )
        {
            return;
        }
        try
        {
            leader.doBatchOfOperations( 1, Operation.createNode );
            consecutiveFailures.set( 0 );
        }
        catch ( Exception e )
        {
            log.error( "MASTER FAILURE " + e );
            if ( consecutiveFailures.incrementAndGet() == config.consecutiveFailuresAllowed() /*limit(15))*/ )
            {
                e.printStackTrace();
                if ( !orchestrator.isInstanceShutdown( leader.getServerId() ) )
                {
                    reporting.fatalError( "Master unresponsive", e );
                }
            }
        }
    }

    public interface Configuration extends Worker.Configuration
    {
        int consecutiveFailuresAllowed();
    }
}
