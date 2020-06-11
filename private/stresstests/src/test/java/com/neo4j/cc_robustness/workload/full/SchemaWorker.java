/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload.full;

import com.neo4j.cc_robustness.CcInstance;
import com.neo4j.cc_robustness.Orchestrator;
import com.neo4j.cc_robustness.workload.SchemaOperation;
import com.neo4j.cc_robustness.workload.SchemaOperationType;
import com.neo4j.cc_robustness.workload.SchemaOperations;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.logging.LogProvider;

/**
 * Will issue schema operations now and then.
 */
public class SchemaWorker extends Worker
{
    private final SchemaOperation[] possibleOperations;
    private volatile boolean workerPaused;
    SchemaWorker( Configuration config, Orchestrator orchestrator, Condition endCondition, RemoteControl remoteControl, ReportingFactory reporting,
            LogProvider logProvider )
    {
        super( "SchemaWorker", config, orchestrator, endCondition, remoteControl, reporting, logProvider );
        this.possibleOperations = collectPossibleOperations( config.schemaOperationTypes() );
        remoteControl.addListener( new RemoteControlListener.Adapter()
        {
            @Override
            public void killerPausedChanged( boolean paused )
            {
                workerPaused = paused;
            }
        } );
    }

    private SchemaOperation[] collectPossibleOperations( Collection<SchemaOperationType> enabledTypes )
    {
        List<SchemaOperation> result = new ArrayList<>();
        for ( SchemaOperation op : SchemaOperations.values() )
        {
            if ( enabledTypes.contains( op.type() ) )
            {
                result.add( op );
            }
        }
        return result.toArray( new SchemaOperation[0] );
    }

    @Override
    protected void doOperationUnderWatchfulEye( CcInstance instance ) throws RemoteException
    {
        SchemaOperation op = possibleOperations[random.nextInt( possibleOperations.length )];
        log.info( "Performing schema operation: " + op );
        instance.doSchemaOperation( op );
    }

    @Override
    protected boolean paused()
    {
        return super.paused() || workerPaused;
    }

    public interface Configuration extends Worker.Configuration
    {
        Collection<SchemaOperationType> schemaOperationTypes();
    }
}
