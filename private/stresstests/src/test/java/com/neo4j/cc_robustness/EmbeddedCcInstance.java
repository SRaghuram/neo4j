/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness;

import com.neo4j.causalclustering.core.state.CoreInstanceInfo;
import com.neo4j.cc_robustness.consistency.RobustnessConsistencyCheck;
import com.neo4j.cc_robustness.util.GraphChecksum;
import com.neo4j.cc_robustness.workload.GraphOperations;
import com.neo4j.cc_robustness.workload.ReferenceNodeStrategy;
import com.neo4j.cc_robustness.workload.SchemaOperation;
import com.neo4j.cc_robustness.workload.ShutdownType;
import com.neo4j.cc_robustness.workload.Work;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Map;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.DbRepresentation;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent.NULL;

public class EmbeddedCcInstance implements CcInstance
{
    private final RobustnessConsistencyCheck consistencyCheck;
    private final int serverId;
    private final File homeDir;
    private final CcInstanceFiles instanceFiles;
    private final CcStartupTimeoutMonitor ccStartupTimeoutMonitor = new CcStartupTimeoutMonitor();
    private final Log log;
    private final SlowLogging slowLogging;
    private volatile DatabaseManagementService managementService;
    private volatile GraphDatabaseAPI db;
    private volatile GraphOperations operations;
    private volatile RuntimeException startupException;

    EmbeddedCcInstance( CcInstanceFiles instanceFiles, int serverId, final ReferenceNodeStrategy referenceNodeStrategy,
            RobustnessConsistencyCheck consistencyCheck, final Map<String,String> additionalDbConfig, LogProvider logProvider, final boolean acquireReadLocks )
    {
        this.instanceFiles = instanceFiles;
        this.serverId = serverId;
        this.homeDir = instanceFiles.directoryFor( serverId );
        this.consistencyCheck = consistencyCheck;
        this.slowLogging = new SlowLogging( additionalDbConfig );
        this.log = logProvider.getLog( getClass() );
        new Thread( () ->
        {
            try
            {
                managementService = startDb( additionalDbConfig );
                db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
                operations = new GraphOperations( db, 5, referenceNodeStrategy, acquireReadLocks );
            }
            catch ( RuntimeException e )
            {
                e.printStackTrace();
                startupException = e;
            }
        } ).start();
    }

    static long getLastCommittedTxId( GraphDatabaseService db )
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();
    }

    static long getLastClosedTxId( GraphDatabaseService db )
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();
    }

    static void rotateLogs( GraphDatabaseService db )
    {
        try
        {
            ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile( NULL );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    static int getNumberOfBranches( GraphDatabaseAPI db )
    {
        return getNumberOfBranches( db.databaseLayout().databaseDirectory() );
    }

    static int getNumberOfBranches( File storeDir )
    {
        File branchDir = new File( storeDir, "branched" );
        if ( !branchDir.exists() )
        {
            return 0;
        }

        int count = 0;
        for ( File child : branchDir.listFiles() )
        {
            if ( child.isDirectory() )
            {
                count++;
            }
        }
        return count;
    }

    @Override
    public void awaitStarted()
    {
        Orchestrator.awaitStarted( () ->
        {
            if ( startupException != null )
            {
                throw startupException;
            }
            return db;
        }, ccStartupTimeoutMonitor, serverId );
    }

    private DatabaseManagementService startDb( Map<String,String> additionalDbConfig )
    {
        return Orchestrator.instantiateDbServer( homeDir, serverId, additionalDbConfig, ccStartupTimeoutMonitor );
    }

    @Override
    public void shutdown( ShutdownType type )
    {
        managementService.shutdown();
        instanceFiles.packDb( serverId, true );
        if ( type == ShutdownType.wipe )
        {
            FileUtils.deleteQuietly( homeDir );
        }
    }

    @Override
    public void doBatchOfOperations( Integer txSize, GraphOperations.Operation... operationOrNoneForRandom )
    {
        operations.doBatchOfOperations( txSize, operationOrNoneForRandom );
    }

    @Override
    public void doSchemaOperation( SchemaOperation... operationOrNoneForRandom )
    {
        operations.doSchemaOperation( operationOrNoneForRandom );
    }

    @Override
    public boolean isLeader()
    {
        return Orchestrator.isLeader( db );
    }

    @Override
    public CoreInstanceInfo coreInfo()
    {
        return Orchestrator.coreInfo( db );
    }

    @Override
    public void dumpLocks()
    {
        Orchestrator.dumpAllLocks( db, instanceFiles.directoryFor( serverId ), log );
    }

    @Override
    public int getServerId()
    {
        return this.serverId;
    }

    @Override
    public void doWorkOnDatabase( Work work )
    {
        work.doWork( db );
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
    public String storeChecksum()
    {
        return new GraphChecksum().checksum( db );
    }

    @Override
    public DbRepresentation representation()
    {
        return DbRepresentation.of( db );
    }

    @Override
    public long getLastCommittedTxId()
    {
        return getLastCommittedTxId( db );
    }

    @Override
    public long getLastClosedTxId()
    {
        return getLastClosedTxId( db );
    }

    @Override
    public void rotateLogs()
    {
        rotateLogs( db );
    }

    @Override
    public int getNumberOfBranches()
    {
        return getNumberOfBranches( db );
    }

    @Override
    public synchronized void createReferenceNode()
    {
        ReferenceNode.createReferenceNode( db );
    }

    @Override
    public synchronized boolean blockNetwork( Integer flags )
    {
        return false;
    }

    @Override
    public synchronized void restoreNetwork()
    {
    }

    @Override
    public boolean isAvailable()
    {
        return Orchestrator.isAvailable( db );
    }

    @Override
    public void log( String message )
    {
        Orchestrator.log( db, message );
    }

    @Override
    public void slowDownLogging( Float probability )
    {
        slowLogging.enable( probability );
    }
}
