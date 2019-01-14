/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.LockingExecutor;

public class CheckPointerService
{
    private final Supplier<CheckPointer> checkPointerSupplier;
    private final Executor lockingCheckpointExecutor;

    public CheckPointerService( Supplier<CheckPointer> checkPointerSupplier, JobScheduler jobScheduler, Group group )
    {
        this.checkPointerSupplier = checkPointerSupplier;
        this.lockingCheckpointExecutor = new LockingExecutor( jobScheduler, group );
    }

    public CheckPointer getCheckPointer()
    {
        return checkPointerSupplier.get();
    }

    public long lastCheckPointedTransactionId()
    {
        return checkPointerSupplier.get().lastCheckPointedTransactionId();
    }

    public void tryAsyncCheckpoint( Consumer<IOException> exceptionHandler )
    {
        lockingCheckpointExecutor.execute( () ->
        {
            try
            {
                getCheckPointer().tryCheckPoint( new SimpleTriggerInfo( "Store file copy" ) );
            }
            catch ( IOException e )
            {
                exceptionHandler.accept( e );
            }
        } );
    }
}
