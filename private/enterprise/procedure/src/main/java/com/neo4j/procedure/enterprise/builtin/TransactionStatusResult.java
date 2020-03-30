/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import org.apache.commons.lang3.StringUtils;

import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;

import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.impl.api.TransactionExecutionStatistic;
import org.neo4j.procedure.builtin.ProceduresTimeFormatHelper;

import static java.lang.String.format;

@SuppressWarnings( "WeakerAccess" )
public class TransactionStatusResult
{
    private static final String RUNNING_STATE = "Running";
    private static final String CLOSING_STATE = "Closing";
    private static final String BLOCKED_STATE = "Blocked by: ";
    private static final String TERMINATED_STATE = "Terminated with reason: %s";

    public final String transactionId;
    public final String username;
    public final Map<String,Object> metaData;
    public final String startTime;
    public final String protocol;
    public final String clientAddress;
    public final String requestUri;

    public final String currentQueryId;
    public final String currentQuery;

    public final long activeLockCount;
    public final String status;
    public Map<String,Object> resourceInformation;

    public final long elapsedTimeMillis;
    public final Long cpuTimeMillis;
    public final long waitTimeMillis;
    public final Long idleTimeMillis;
    public final Long allocatedBytes;
    public final Long allocatedDirectBytes;
    public final long pageHits;
    public final long pageFaults;
    /** @since Neo4j 3.5 */
    public final String connectionId;
    public final String initializationStackTrace;
    /** @since Neo4j 4.0 */
    public final String database;
    /** @since Neo4j 4.1 */
    public final Long estimatedUsedHeapMemory;

    public TransactionStatusResult( String database, KernelTransactionHandle transaction,
            TransactionDependenciesResolver transactionDependenciesResolver,
            Map<KernelTransactionHandle,Optional<QuerySnapshot>> handleSnapshotsMap, ZoneId zoneId ) throws InvalidArgumentsException
    {
        this.database = database;
        this.transactionId = new DbmsTransactionId( database, transaction.getUserTransactionId() ).toString();
        this.username = transaction.subject().username();
        this.startTime = ProceduresTimeFormatHelper.formatTime( transaction.startTime(), zoneId );
        this.activeLockCount = transaction.activeLocks().count();
        Optional<QuerySnapshot> querySnapshot = handleSnapshotsMap.get( transaction );
        TransactionExecutionStatistic statistic = transaction.transactionStatistic();
        elapsedTimeMillis = statistic.getElapsedTimeMillis();
        cpuTimeMillis = statistic.getCpuTimeMillis();
        allocatedBytes = statistic.getHeapAllocatedBytes();
        allocatedDirectBytes = statistic.getNativeAllocatedBytes();
        estimatedUsedHeapMemory = statistic.getEstimatedUsedHeapMemory();
        waitTimeMillis = statistic.getWaitTimeMillis();
        idleTimeMillis = statistic.getIdleTimeMillis();
        pageHits = statistic.getPageHits();
        pageFaults = statistic.getPageFaults();

        if ( querySnapshot.isPresent() )
        {
            QuerySnapshot snapshot = querySnapshot.get();
            this.currentQueryId = new DbmsQueryId( database, snapshot.internalQueryId() ).toString();
            this.currentQuery = snapshot.obfuscatedQueryText().orElse( null );
        }
        else
        {
            this.currentQueryId = StringUtils.EMPTY;
            this.currentQuery = StringUtils.EMPTY;
        }
        ClientConnectionInfo clientInfo = transaction.clientInfo();
        this.protocol = clientInfo.protocol();
        this.clientAddress = clientInfo.clientAddress();
        this.requestUri = clientInfo.requestURI();
        this.connectionId = clientInfo.connectionId();
        this.resourceInformation = transactionDependenciesResolver.describeBlockingLocks( transaction );
        this.status = getStatus( transaction, transactionDependenciesResolver );
        this.metaData = transaction.getMetaData();
        this.initializationStackTrace = transaction.transactionInitialisationTrace().getTrace();
    }

    private static String getStatus( KernelTransactionHandle handle, TransactionDependenciesResolver transactionDependenciesResolver )
    {
        return handle.terminationReason().map( reason -> format( TERMINATED_STATE, reason.code() ) )
                .orElseGet( () -> getExecutingStatus( handle, transactionDependenciesResolver ) );
    }

    private static String getExecutingStatus( KernelTransactionHandle handle, TransactionDependenciesResolver transactionDependenciesResolver )
    {
        if ( transactionDependenciesResolver.isBlocked( handle ) )
        {
            return BLOCKED_STATE + transactionDependenciesResolver.describeBlockingTransactions( handle );
        }
        else if ( handle.isClosing() )
        {
            return CLOSING_STATE;
        }
        return RUNNING_STATE;
    }
}
