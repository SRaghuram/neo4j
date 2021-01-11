/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.configuration.TransactionStreamingStrategy;

import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.SettingChangeListener;

public class TransactionStreamingStrategyProvider
        implements SettingChangeListener<TransactionStreamingStrategy>, Supplier<TransactionStreamingStrategy>
{
    private volatile TransactionStreamingStrategy currentStrategy;

    public static TransactionStreamingStrategyProvider register( Config config )
    {
        return new TransactionStreamingStrategyProvider( config );
    }

    private TransactionStreamingStrategyProvider( Config config )
    {
        currentStrategy = config.get( OnlineBackupSettings.incremental_backup_strategy );
        config.addListener( OnlineBackupSettings.incremental_backup_strategy, this );
    }

    @Override
    public void accept( TransactionStreamingStrategy before, TransactionStreamingStrategy after )
    {
        currentStrategy = after;
    }

    @Override
    public TransactionStreamingStrategy get()
    {
        return currentStrategy;
    }
}
