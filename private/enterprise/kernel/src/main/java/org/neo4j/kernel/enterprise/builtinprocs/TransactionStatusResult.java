/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.enterprise.builtinprocs;

import org.apache.commons.lang3.StringUtils;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.impl.api.TransactionExecutionStatistic;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;

import static java.lang.String.format;
import static org.neo4j.kernel.enterprise.builtinprocs.QueryId.ofInternalId;

@SuppressWarnings( "WeakerAccess" )
public class TransactionStatusResult
{
    private static final String RUNNING_STATE = "Running";
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

    public TransactionStatusResult( KernelTransactionHandle transaction,
            TransactionDependenciesResolver transactionDependenciesResolver,
            Map<KernelTransactionHandle,List<QuerySnapshot>> handleSnapshotsMap, ZoneId zoneId ) throws InvalidArgumentsException
    {
        this.transactionId = transaction.getUserTransactionName();
        this.username = transaction.subject().username();
        this.startTime = ProceduresTimeFormatHelper.formatTime( transaction.startTime(), zoneId );
        Optional<Status> terminationReason = transaction.terminationReason();
        this.activeLockCount = transaction.activeLocks().count();
        List<QuerySnapshot> querySnapshots = handleSnapshotsMap.get( transaction );
        TransactionExecutionStatistic statistic = transaction.transactionStatistic();
        elapsedTimeMillis = statistic.getElapsedTimeMillis();
        cpuTimeMillis = statistic.getCpuTimeMillis();
        allocatedBytes = statistic.getHeapAllocatedBytes();
        allocatedDirectBytes = statistic.getDirectAllocatedBytes();
        waitTimeMillis = statistic.getWaitTimeMillis();
        idleTimeMillis = statistic.getIdleTimeMillis();
        pageHits = statistic.getPageHits();
        pageFaults = statistic.getPageFaults();

        if ( !querySnapshots.isEmpty() )
        {
            QuerySnapshot snapshot = querySnapshots.get( 0 );
            ClientConnectionInfo clientConnectionInfo = snapshot.clientConnection();
            this.currentQueryId = ofInternalId( snapshot.internalQueryId() ).toString();
            this.currentQuery = snapshot.queryText();
            this.protocol = clientConnectionInfo.protocol();
            this.clientAddress = clientConnectionInfo.clientAddress();
            this.requestUri = clientConnectionInfo.requestURI();
            this.connectionId = clientConnectionInfo.connectionId();
        }
        else
        {
            this.currentQueryId = StringUtils.EMPTY;
            this.currentQuery = StringUtils.EMPTY;
            this.protocol = StringUtils.EMPTY;
            this.clientAddress = StringUtils.EMPTY;
            this.requestUri = StringUtils.EMPTY;
            this.connectionId = StringUtils.EMPTY;
        }
        this.resourceInformation = transactionDependenciesResolver.describeBlockingLocks( transaction );
        this.status = getStatus( transaction, terminationReason, transactionDependenciesResolver );
        this.metaData = transaction.getMetaData();
        this.initializationStackTrace = transaction.transactionInitialisationTrace().getTrace();
    }

    private static String getStatus( KernelTransactionHandle handle, Optional<Status> terminationReason,
            TransactionDependenciesResolver transactionDependenciesResolver )
    {
        return terminationReason.map( reason -> format( TERMINATED_STATE, reason.code() ) )
                .orElseGet( () -> getExecutingStatus( handle, transactionDependenciesResolver ) );
    }

    private static String getExecutingStatus( KernelTransactionHandle handle, TransactionDependenciesResolver transactionDependenciesResolver )
    {
        return transactionDependenciesResolver.isBlocked( handle ) ? "Blocked by: " +
                transactionDependenciesResolver.describeBlockingTransactions( handle ) : RUNNING_STATE;
    }
}
