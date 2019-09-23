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
import java.util.function.Consumer;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;

class RxPooledDriver extends PooledDriver
{

    private final Driver driver;

    RxPooledDriver( Driver driver, Consumer<PooledDriver> releaseCallback )
    {
        super( driver, releaseCallback );
        this.driver = driver;
    }

    public Mono<FabricDriverTransaction> beginTransaction( FabricConfig.Graph location, AccessMode accessMode, FabricTransactionInfo transactionInfo )
    {
        var sessionConfig = createSessionConfig( location, accessMode );
        var session = driver.rxSession( sessionConfig );

        var driverTransaction = getDriverTransaction( session, transactionInfo );

        return driverTransaction.map( tx ->  new FabricDriverRxTransaction( tx, session, location ));
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
