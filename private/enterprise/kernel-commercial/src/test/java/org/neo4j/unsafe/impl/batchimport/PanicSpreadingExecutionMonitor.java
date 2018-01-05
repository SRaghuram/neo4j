/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.StageExecution;

import static java.lang.System.currentTimeMillis;

class PanicSpreadingExecutionMonitor implements ExecutionMonitor
{
    private final String stageName;
    private final boolean trueForStart;

    PanicSpreadingExecutionMonitor( String stageName, boolean trueForStart )
    {
        this.stageName = stageName;
        this.trueForStart = trueForStart;
    }

    @Override
    public void start( StageExecution execution )
    {
        if ( trueForStart && stageName.equals( execution.getStageName() ) )
        {
            execution.panic( new RuntimeException( "Deliberately causing import to fail at start of stage " + stageName ) );
        }
    }

    @Override
    public long nextCheckTime()
    {
        return currentTimeMillis() + 100;
    }

    @Override
    public void end( StageExecution execution, long totalTimeMillis )
    {
        if ( !trueForStart && stageName.equals( execution.getStageName() ) )
        {
            execution.panic( new RuntimeException( "Deliberately causing import to fail at end of stage " + stageName ) );
        }
    }

    @Override
    public void done( long totalTimeMillis, String additionalInformation )
    {
    }

    @Override
    public void check( StageExecution execution )
    {
    }
}
