/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import com.neo4j.fabric.driver.FabricDriverTransaction;
import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.driver.DriverPool;
import com.neo4j.fabric.driver.PooledDriver;
import com.neo4j.fabric.planner.api.Plan.QueryTask.QueryMode;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.transaction.FabricTransactionInfo;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.bolt.runtime.AccessMode;

import org.neo4j.values.virtual.MapValue;

public class FabricRemoteExecutor
{
    private final DriverPool driverPool;

    public FabricRemoteExecutor( DriverPool driverPool )
    {
        this.driverPool = driverPool;
    }

    public FabricRemoteTransaction begin( FabricTransactionInfo transactionInfo )
    {
        return new FabricRemoteTransaction( transactionInfo );
    }

    public class FabricRemoteTransaction
    {
        private final FabricTransactionInfo transactionInfo;
        private final Map<FabricConfig.Graph,PooledDriver> usedDrivers = new HashMap<>();
        private FabricConfig.Graph writingTo;
        private Mono<FabricDriverTransaction> writeTransaction;

        private FabricRemoteTransaction( FabricTransactionInfo transactionInfo )
        {
            this.transactionInfo = transactionInfo;
        }

        public Mono<StatementResult> run( FabricConfig.Graph location, String query, QueryMode mode, MapValue params )
        {
            if ( location.equals( writingTo ) )
            {
                return runInWriteTransaction( query, params );
            }

            var accessMode = getAccessMode( mode );

            if ( accessMode == AccessMode.READ )
            {
                return runInAutoCommitReadTransaction( location, query, params );
            }

            if ( writingTo != null && !writingTo.equals( location ) )
            {
                throw multipleWriteError( location, writingTo );
            }

            beginWriteTransaction( location );
            return runInWriteTransaction( query, params );
        }

        public Mono<Void> commit()
        {
            return endTransaction( FabricDriverTransaction::commit );
        }

        public Mono<Void> rollback()
        {
            return endTransaction( FabricDriverTransaction::rollback );
        }

        private Mono<Void> endTransaction( Function<FabricDriverTransaction,Mono<Void>> operation )
        {
            if ( writeTransaction == null )
            {
                releaseTransactionResources();
                return Mono.empty();
            }
            return writeTransaction.flatMap( operation ).doFinally( signal -> releaseTransactionResources() );
        }

        private void beginWriteTransaction( FabricConfig.Graph location )
        {
            writingTo = location;
            var driver = getDriver( location );
            writeTransaction = driver.beginTransaction( location, AccessMode.WRITE, transactionInfo, List.of() );
        }

        private Mono<StatementResult> runInAutoCommitReadTransaction( FabricConfig.Graph location, String query, MapValue params )
        {
            var driver = getDriver( location );
            var autoCommitStatementResult = driver.run( query, params, location, AccessMode.READ, transactionInfo, List.of() );
            return Mono.just( autoCommitStatementResult );
        }

        private Mono<StatementResult> runInWriteTransaction( String query, MapValue params )
        {
            return writeTransaction.map( rxTransaction -> rxTransaction.run( query, params ) );
        }

        private AccessMode getAccessMode( QueryMode queryMode )
        {
            if ( transactionInfo.getAccessMode() == AccessMode.READ || queryMode == QueryMode.CAN_READ_ONLY )
            {
                return AccessMode.READ;
            }

            return AccessMode.WRITE;
        }

        private PooledDriver getDriver( FabricConfig.Graph location )
        {
            return usedDrivers.computeIfAbsent( location, l -> driverPool.getDriver( location, transactionInfo.getLoginContext().subject() ) );
        }

        private UnsupportedOperationException multipleWriteError( FabricConfig.Graph attempt, FabricConfig.Graph writingTo )
        {
            return new UnsupportedOperationException( String.format( "Multi-shard writes not allowed. Attempted write to %s, currently writing to %s",
                    attempt,
                    writingTo ) );
        }

        private void releaseTransactionResources()
        {
            usedDrivers.values().forEach( PooledDriver::release );
        }
    }
}
