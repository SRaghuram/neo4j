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

import java.io.File;
import java.rmi.RemoteException;
import java.util.Map;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.test.DbRepresentation;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

/**
 * |---------------------------------------------------------|      |-------------|
 * |                MAIN CC-ROBUSTNESS JVM                   |      | DB INSTANCE |
 * |---------------------------------------------------------|      |-------------|
 * | (main process for CcRobustness) <==> (CcProcessWrapper) | <==> | (CcProcess) |
 * |---------------------------------------------------------|      |-------------|
 * <p/>
 * A {@link CcProcess} sits in its own jvm, controlled by a {@link CcProcessWrapper} from the main jvm.
 */
public class CcProcess extends SubProcess<CcInstance,Map<String,String>> implements CcInstance
{
    public static final String SERVER_ID_KEY = "server_id";

    private transient RobustnessConsistencyCheck consistencyCheck;
    private transient volatile GraphDatabaseAPI db;
    private volatile GraphOperations operations;
    private volatile int serverId;
    private volatile RuntimeException startupException;
    private CcStartupTimeoutMonitor ccStartupTimeoutMonitor;
    private Log log;
    private Restoration currentBlock;
    private SlowLogging slowLogging;
    private File homeDir;
    private DatabaseManagementService managementService;

    @Override
    protected void startup( Map<String,String> parameters )
    {
        try
        {
            homeDir = new File( parameters.get( "dbdir" ) );

            ccStartupTimeoutMonitor = new CcStartupTimeoutMonitor();
            log = FormattedLogProvider.toOutputStream( System.out ).getLog( getClass() );
            consistencyCheck = new RobustnessConsistencyCheck( log );
            serverId = Integer.parseInt( parameters.get( SERVER_ID_KEY ) );
            boolean grabReadLocks = Boolean.parseBoolean( parameters.get( "acquireReadLocks" ) );
            ReferenceNodeStrategy referenceNodeStrategy = ReferenceNodeStrategy.valueOf( parameters.remove( "referencenode" ) );
            slowLogging = new SlowLogging( parameters );
            managementService = Orchestrator.instantiateDbServer( homeDir, serverId, parameters, ccStartupTimeoutMonitor );
            db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
            operations = new GraphOperations( db, 5, referenceNodeStrategy, grabReadLocks );
        }
        catch ( RuntimeException e )
        {
            e.printStackTrace();
            startupException = e;
        }
    }

    @Override
    public void awaitStarted()
    {
        while ( ccStartupTimeoutMonitor == null )
        {
            try
            {
                Thread.sleep( 100 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }

        Orchestrator.awaitStarted( () ->
        {
            if ( startupException != null )
            {
                throw startupException;
            }
            return db;
        }, ccStartupTimeoutMonitor, serverId );
    }

    @Override
    public int getServerId()
    {
        return serverId;
    }

    @Override
    public void doWorkOnDatabase( Work work )
    {
        work.doWork( db );
    }

    @Override
    public void doBatchOfOperations( Integer txSize, GraphOperations.Operation... operationOrNoneForRandom )
    {
        operations.doBatchOfOperations( txSize, operationOrNoneForRandom );
    }

    @Override
    public void doSchemaOperation( SchemaOperation... operationOrNoneForRandom ) throws RemoteException
    {
        operations.doSchemaOperation( operationOrNoneForRandom );
    }

    @Override
    public void shutdown( ShutdownType type )
    {
//        Orchestrator.packDb( serverId );
        if ( type != ShutdownType.harsh )
        {
            managementService.shutdown();
        }
        shutdown();
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
        Orchestrator.dumpAllLocks( db, homeDir, log );
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
    public DbRepresentation representation() throws RemoteException
    {
        return DbRepresentation.of( db );
    }

    @Override
    public long getLastCommittedTxId() throws RemoteException
    {
        return EmbeddedCcInstance.getLastCommittedTxId( db );
    }

    @Override
    public long getLastClosedTxId() throws RemoteException
    {
        return EmbeddedCcInstance.getLastClosedTxId( db );
    }

    @Override
    public void rotateLogs() throws RemoteException
    {
        EmbeddedCcInstance.rotateLogs( db );
    }

    @Override
    public int getNumberOfBranches()
    {
        return EmbeddedCcInstance.getNumberOfBranches( db );
    }

    @Override
    public void createReferenceNode()
    {
        ReferenceNode.createReferenceNode( db );
    }

    @Override
    public synchronized boolean blockNetwork( Integer flags )
    {
        return true;
    }

    @Override
    public synchronized void restoreNetwork() throws RemoteException
    {
        currentBlock = null;
    }

    @Override
    public boolean isAvailable()
    {
        if ( db == null )
        {
            // Return true since the result of this call is used to print a warning about this instance
            // not being available. If the process is starting up and db hasn't yet been assigned
            // there are already other status denoting that.
            return true;
        }
        return Orchestrator.isAvailable( db );
    }

    @Override
    public void log( String message ) throws RemoteException
    {
        Orchestrator.log( db, message );
    }

    @Override
    public void slowDownLogging( Float probability )
    {
        slowLogging.enable( probability );
    }
}
