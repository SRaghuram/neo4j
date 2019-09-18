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
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.internal.SessionConfig;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;

public class PooledDriver
{
    private final Driver driver;
    private final AtomicInteger referenceCounter = new AtomicInteger();
    private final Consumer<PooledDriver> releaseCallback;
    private Instant lastUsedTimestamp;

    PooledDriver( Driver driver, Consumer<PooledDriver> releaseCallback )
    {
        this.driver = driver;
        this.releaseCallback = releaseCallback;
    }

    public void release()
    {
        releaseCallback.accept( this );
    }

    public Mono<FabricDriverTransaction> beginTransaction( FabricConfig.Graph location, AccessMode accessMode, FabricTransactionInfo transactionInfo )
    {
        var session = driver.rxSession( SessionConfig.builder()
                .withDefaultAccessMode( translateAccessMode(accessMode) )
                .withDatabase( location.getDatabase() )
                .build() );

        var driverTransaction = getDriverTransaction( session, transactionInfo );

        return driverTransaction.map( tx ->  new FabricDriverTransaction( tx, session, location ));
    }

    AtomicInteger getReferenceCounter()
    {
        return referenceCounter;
    }

    Instant getLastUsedTimestamp()
    {
        return lastUsedTimestamp;
    }

    void setLastUsedTimestamp( Instant lastUsedTimestamp )
    {
        this.lastUsedTimestamp = lastUsedTimestamp;
    }

    void close()
    {
        driver.close();
    }

    private org.neo4j.driver.AccessMode translateAccessMode( AccessMode accessMode )
    {
        if ( accessMode == AccessMode.READ )
        {
            return org.neo4j.driver.AccessMode.READ;
        }

        return org.neo4j.driver.AccessMode.WRITE;
    }

    private Mono<RxTransaction> getDriverTransaction( RxSession session, FabricTransactionInfo transactionInfo )
    {
        if ( transactionInfo.getTxTimeout().equals( Duration.ZERO ) )
        {

            return Mono.from( session.beginTransaction() ).cache();
        }

        return Mono.from( session.beginTransaction( TransactionConfig.builder().withTimeout( transactionInfo.getTxTimeout() ).build() ) ).cache();
    }
}
