/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.batchimport;

import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.batchimport.staging.StageExecution;

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
    public void start( StageControl execution )
    {
        if ( trueForStart && stageName.equals( ((StageExecution)execution).getStageName() ) )
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
    public void end( StageControl execution, long totalTimeMillis )
    {
        if ( !trueForStart && stageName.equals( ((StageExecution)execution).getStageName() ) )
        {
            execution.panic( new RuntimeException( "Deliberately causing import to fail at end of stage " + stageName ) );
        }
    }

    @Override
    public void done( boolean successful, long totalTimeMillis, String additionalInformation )
    {
    }

    @Override
    public void check( StageControl execution )
    {
    }
}
