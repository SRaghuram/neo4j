/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness;

import com.neo4j.causalclustering.core.state.CoreInstanceInfo;
import com.neo4j.cc_robustness.consistency.RobustnessConsistencyCheck;
import com.neo4j.cc_robustness.workload.GraphOperations;
import com.neo4j.cc_robustness.workload.SchemaOperation;
import com.neo4j.cc_robustness.workload.ShutdownType;
import com.neo4j.cc_robustness.workload.Work;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.NamedThreadFactory;

import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.neo4j.test.DbRepresentation;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * |---------------------------------------------------------|      |-------------|
 * |                MAIN CC-ROBUSTNESS JVM                   |      | DB INSTANCE |
 * |---------------------------------------------------------|      |-------------|
 * | (main process for CcRobustness) <==> (CcProcessWrapper) | <==> | (CcProcess) |
 * |---------------------------------------------------------|      |-------------|
 * <p/>
 * A {@link CcProcessWrapper} sits in the main jvm, abstracting the fact that the real db instance sits
 * it's a sub process.
 */
public class CcProcessWrapper implements CcInstance
{
    private final RobustnessConsistencyCheck consistencyCheck;
    private final CcInstance process;
    private final CcInstanceFiles instanceFiles;
    private final Path homeDir;

    CcProcessWrapper( CcInstanceFiles instanceFiles, RobustnessConsistencyCheck consistencyCheck, CcInstance process, Map<String,String> parameters )
    {
        this.instanceFiles = instanceFiles;
        this.consistencyCheck = consistencyCheck;
        this.process = process;
        this.homeDir = Path.of( parameters.get( "dbdir" ) );
    }

    @Override
    public void awaitStarted() throws RemoteException
    {
        process.awaitStarted();
    }

    @Override
    public int getServerId() throws RemoteException
    {
        return process.getServerId();
    }

    @Override
    public void doWorkOnDatabase( Work work ) throws RemoteException
    {
        process.doWorkOnDatabase( work );
    }

    @Override
    public void doBatchOfOperations( Integer txSize, GraphOperations.Operation... operationOrNoneForRandom ) throws RemoteException
    {
        process.doBatchOfOperations( txSize, operationOrNoneForRandom );
    }

    @Override
    public void doSchemaOperation( SchemaOperation... operationOrNoneForRandom ) throws RemoteException
    {
        process.doSchemaOperation( operationOrNoneForRandom );
    }

    @Override
    public void shutdown( final ShutdownType type ) throws RemoteException
    {
        // Tell the process to shut down... and let's wait for it a while to do so
        int serverId = process.getServerId();
        ExecutorService executor = newSingleThreadExecutor( new NamedThreadFactory( "CC process db shutdown" ) );
        Future<Void> shutdownFuture = executor.submit( () ->
        {
            try
            {
                process.shutdown( type );
            }
            catch ( IllegalStateException e )
            {
                // This is expected because SubProcess.shutdown will throw it.
                // Maybe I'm using SubProcess in an invalid way.
            }
            return null;
        } );

        boolean killIt = false;
        try
        {
            shutdownFuture.get( 2, MINUTES );
        }
        catch ( InterruptedException | TimeoutException e )
        {
            killIt = true;
        }
        catch ( ExecutionException e )
        {
            throw new RuntimeException( "Waiting for instance to shut down failed!", e );
        }
        finally
        {
            shutdownFuture.cancel( true );
        }

        if ( killIt )
        {
            Future<?> killFuture = executor.submit( () -> SubProcess.kill( process ) );
            try
            {
                killFuture.get( 2, MINUTES );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( "Waiting for instance to shut down failed!", e );
            }
        }

        // We asked the subprocess to shutdown or kill with effectively a `kill -15`,
        // meaning shutdown hooks would be allowed to run. At this point here in the calling code all we know is that
        // we lost remote connectivity, not that the process was necessarily shut down. So we sleep for a while - best
        // effort robustness...
        try
        {
            Thread.sleep( 120000 );
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace();
        }

        // And pack the db and stuff like that
        try
        {
            instanceFiles.packDb( serverId, true );
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( "Failed to pack database. Debug data: killit=" + killIt + ", type=" + type + ", serverId=" + serverId, e );
        }

        if ( type == ShutdownType.wipe )
        {
            Path dbPath = instanceFiles.directoryFor( serverId );
            FileUtils.deleteQuietly( dbPath.toFile() );
        }

        executor.shutdownNow();
    }

    @Override
    public boolean isLeader() throws RemoteException
    {
        try
        {
            return process.isLeader();
        }
        catch ( IllegalStateException e )
        {
            // Can be caused by the sub process shutting down or similar
            return false;
        }
    }

    @Override
    public CoreInstanceInfo coreInfo()
    {
        return process.coreInfo();
    }

    @Override
    public void dumpLocks() throws RemoteException
    {
        process.dumpLocks();
    }

    @Override
    public void verifyConsistencyOffline() throws RemoteException
    {
        try
        {
            consistencyCheck.verifyConsistencyOffline( homeDir );
        }
        catch ( Exception e )
        {
            throw new RemoteException( "Failed to check consistency", e );
        }
    }

    @Override
    public String storeChecksum() throws RemoteException
    {
        return process.storeChecksum();
    }

    @Override
    public DbRepresentation representation() throws RemoteException
    {
        return process.representation();
    }

    @Override
    public long getLastCommittedTxId() throws RemoteException
    {
        return process.getLastCommittedTxId();
    }

    @Override
    public long getLastClosedTxId() throws RemoteException
    {
        return process.getLastClosedTxId();
    }

    @Override
    public void rotateLogs() throws RemoteException
    {
        process.rotateLogs();
    }

    @Override
    public int getNumberOfBranches() throws RemoteException
    {
        return process.getNumberOfBranches();
    }

    @Override
    public void createReferenceNode() throws RemoteException
    {
        process.createReferenceNode();
    }

    @Override
    public boolean blockNetwork( Integer flags ) throws RemoteException
    {
        return process.blockNetwork( flags );
    }

    @Override
    public void restoreNetwork() throws RemoteException
    {
        process.restoreNetwork();
    }

    @Override
    public boolean isAvailable() throws RemoteException
    {
        try
        {
            return process.isAvailable();
        }
        catch ( ConnectionDisruptedException e )
        {
            // Return true since the result of this call is used to print a warning about this instance
            // not being available. If the process is dead or shutting down there are already other status
            // denoting that.
            return true;
        }
    }

    @Override
    public void log( String message ) throws RemoteException
    {
        process.log( message );
    }

    @Override
    public void slowDownLogging( Float probability ) throws RemoteException
    {
        process.slowDownLogging( probability );
    }
}
