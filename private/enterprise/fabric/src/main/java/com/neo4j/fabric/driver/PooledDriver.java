/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.fabric.bookmark.RemoteBookmark;
import org.neo4j.fabric.executor.ExecutionOptions;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
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

    public abstract AutoCommitStatementResult run( String query, MapValue params, Location.Remote location, ExecutionOptions options,
            AccessMode accessMode, FabricTransactionInfo transactionInfo, List<RemoteBookmark> bookmarks  );

    public abstract Mono<FabricDriverTransaction> beginTransaction( Location.Remote location, ExecutionOptions options, AccessMode accessMode,
            FabricTransactionInfo transactionInfo, List<RemoteBookmark> bookmarks );

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

    protected SessionConfig createSessionConfig( Location.Remote location, AccessMode accessMode, List<RemoteBookmark> bookmarks )
    {
        var builder = SessionConfig.builder().withDefaultAccessMode( translateAccessMode( accessMode ) );

        var mergedBookmarks = new HashSet<String>();
        bookmarks.forEach( remoteBookmark -> mergedBookmarks.add( remoteBookmark.getSerialisedState() ) );

        builder.withBookmarks(  Bookmark.from( mergedBookmarks ) );

        if ( location.getDatabaseName() != null )
        {
            builder.withDatabase( location.getDatabaseName() );
        }

        return builder.build();
    }

    protected TransactionConfig getTransactionConfig( FabricTransactionInfo transactionInfo )
    {
        var builder = TransactionConfig.builder();

        if ( !transactionInfo.getTxTimeout().equals( Duration.ZERO ) )
        {
            builder.withTimeout( transactionInfo.getTxTimeout() );
        }

        if ( transactionInfo.getTxMetadata() != null )
        {
            builder.withMetadata( transactionInfo.getTxMetadata() );
        }

        return builder.build();
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
