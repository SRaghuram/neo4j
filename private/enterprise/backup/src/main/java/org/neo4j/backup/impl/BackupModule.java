/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;

import java.time.Clock;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;

import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

public class BackupModule
{
    private final LogProvider logProvider;
    private final FileSystemAbstraction fs;
    private final Monitors monitors;
    private final StorageEngineFactory storageEngineFactory;
    private final Clock clock;
    private final TransactionLogCatchUpFactory transactionLogCatchUpFactory;
    private final JobScheduler jobScheduler;

    /**
     * Dependencies that can be resolved immediately after launching the backup tool
     *
     * @param fs the file system for backup
     * @param logProvider made available to subsequent dependency resolution classes
     * @param monitors will become shared across all resolved dependencies
     */
    public BackupModule( FileSystemAbstraction fs, LogProvider logProvider, Monitors monitors, StorageEngineFactory storageEngineFactory )
    {
        this.logProvider = logProvider;
        this.monitors = monitors;
        this.storageEngineFactory = storageEngineFactory;
        this.clock = Clock.systemDefaultZone();
        this.transactionLogCatchUpFactory = new TransactionLogCatchUpFactory();
        this.fs = fs;
        this.jobScheduler = createInitialisedScheduler();
    }

    public LogProvider getLogProvider()
    {
        return logProvider;
    }

    public FileSystemAbstraction getFileSystem()
    {
        return fs;
    }

    public Monitors getMonitors()
    {
        return monitors;
    }

    public Clock getClock()
    {
        return clock;
    }

    public TransactionLogCatchUpFactory getTransactionLogCatchUpFactory()
    {
        return transactionLogCatchUpFactory;
    }

    public JobScheduler jobScheduler()
    {
        return jobScheduler;
    }

    public StorageEngineFactory getStorageEngineFactory()
    {
        return storageEngineFactory;
    }
}
