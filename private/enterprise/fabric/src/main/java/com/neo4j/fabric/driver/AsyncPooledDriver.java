/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.transaction.FabricTransactionInfo;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;

public class AsyncPooledDriver extends PooledDriver
{

    private final Driver driver;

    AsyncPooledDriver( Driver driver, Consumer<PooledDriver> releaseCallback )
    {
        super( driver, releaseCallback );
        this.driver = driver;
    }

    @Override
    public Mono<FabricDriverTransaction> beginTransaction( FabricConfig.Graph location, AccessMode accessMode, FabricTransactionInfo transactionInfo )
    {
        var sessionConfig = createSessionConfig( location, accessMode );
        var session = driver.asyncSession( sessionConfig );

        var driverTransaction = getDriverTransaction( session, transactionInfo );

        return Mono.fromFuture( driverTransaction.toCompletableFuture() ).map( tx ->  new FabricDriverAsyncTransaction( tx, session, location ));
    }

    private CompletionStage<AsyncTransaction> getDriverTransaction( AsyncSession session, FabricTransactionInfo transactionInfo )
    {
        if ( transactionInfo.getTxTimeout().equals( Duration.ZERO ) )
        {
            return session.beginTransactionAsync();
        }

        return session.beginTransactionAsync( TransactionConfig.builder().withTimeout( transactionInfo.getTxTimeout() ).build() );
    }
}
