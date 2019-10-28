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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.values.virtual.MapValue;

public abstract class PooledDriver
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

    public abstract AutoCommitStatementResult run( String query, MapValue params, FabricConfig.Graph location, AccessMode accessMode,
            FabricTransactionInfo transactionInfo, List<String> bookmarks  );

    public abstract Mono<FabricDriverTransaction> beginTransaction( FabricConfig.Graph location, AccessMode accessMode, FabricTransactionInfo transactionInfo,
            List<String> bookmarks );

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

    protected SessionConfig createSessionConfig( FabricConfig.Graph location, AccessMode accessMode, List<String> bookmarks )
    {
        var builder = SessionConfig.builder().withDefaultAccessMode( translateAccessMode( accessMode ) );

        var convertedBookmarks = bookmarks.stream().map( DriverBookmarkFormat::parse ).collect( Collectors.toList() );
        builder.withBookmarks( convertedBookmarks );

        if ( location.getDatabase() != null )
        {
            builder.withDatabase( location.getDatabase() );
        }

        return builder.build();
    }

    protected TransactionConfig getTransactionConfig( FabricTransactionInfo transactionInfo )
    {
        if ( transactionInfo.getTxTimeout().equals( Duration.ZERO ) )
        {
            return TransactionConfig.empty();
        }

        return TransactionConfig.builder().withTimeout( transactionInfo.getTxTimeout() ).build();
    }

    private org.neo4j.driver.AccessMode translateAccessMode( AccessMode accessMode )
    {
        if ( accessMode == AccessMode.READ )
        {
            return org.neo4j.driver.AccessMode.READ;
        }

        return org.neo4j.driver.AccessMode.WRITE;
    }
}
