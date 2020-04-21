/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.fabric;

import com.neo4j.fabric.bolt.BoltFabricDatabaseService;
import com.neo4j.fabric.executor.FabricExecutor;
import com.neo4j.fabric.transaction.FabricTransaction;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static java.util.Objects.requireNonNull;

public class TestFabricGraphDatabaseService extends GraphDatabaseFacade
{
    private static final AtomicLong TRANSACTION_COUNTER = new AtomicLong();
    private static final String TAG_NAME = "fabric-tx-id";

    final BoltGraphDatabaseServiceSPI boltFabricDatabaseService;
    final Config config;

    public TestFabricGraphDatabaseService( GraphDatabaseFacade baseDb,
                                           BoltGraphDatabaseServiceSPI boltFabricDatabaseService,
                                           Config config )
    {
        super( baseDb, Function.identity() );
        this.boltFabricDatabaseService = boltFabricDatabaseService;
        this.config = requireNonNull( config );
    }

    @Override
    protected InternalTransaction beginTransactionInternal( KernelTransaction.Type type,
                                                            LoginContext loginContext,
                                                            ClientConnectionInfo connectionInfo,
                                                            long timeoutMillis )
    {

        var fabricTxId = TRANSACTION_COUNTER.incrementAndGet();
        var boltTransaction = boltFabricDatabaseService.beginTransaction( type, loginContext, connectionInfo, List.of(),
                                                                          Duration.ofMillis( timeoutMillis ), AccessMode.WRITE,
                                                                          Map.of( TAG_NAME, fabricTxId ) );
        var internalTransaction = forceKernelTxCreation( boltTransaction );
        return new TestFabricTransaction( contextFactory, boltTransaction, internalTransaction );
    }

    private InternalTransaction forceKernelTxCreation( BoltTransaction boltTransaction )
    {
        FabricExecutor fabricExecutor = getDependencyResolver().resolveDependency( FabricExecutor.class );
        FabricTransaction fabricTransaction = ((BoltFabricDatabaseService.BoltTransactionImpl) boltTransaction).getFabricTransaction();
        return fabricExecutor.forceKernelTxCreation( fabricTransaction );
    }
}