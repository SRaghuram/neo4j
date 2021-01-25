/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload.full;

import com.neo4j.cc_robustness.Orchestrator;

import org.neo4j.logging.LogProvider;

class RotatorWorker extends Worker
{
    RotatorWorker( Configuration config, Orchestrator orchestrator, Condition endCondition, RemoteControl remoteControl, ReportingFactory reporting,
            LogProvider logProvider )
    {
        super( "Rotator", config, orchestrator, endCondition, remoteControl, reporting, logProvider );
        log.info( "Extra rotation of nioneo log is started" );
    }

    @Override
    protected void doOneOperation()
    {
        try
        {
            orchestrator.getLeaderInstance().rotateLogs();
        }
        catch ( Throwable e )
        {
            // TODO shall we consider some exceptions fatal (i.e. throw them further)?
            log.info( getName() + " couldn't rotate this time, trying again later " + e );
            e.printStackTrace();
        }
    }
}
