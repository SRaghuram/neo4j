/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.time.Clock;

import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

public class BackupModule
{
    private final OutsideWorld outsideWorld;
    private final LogProvider logProvider;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final Monitors monitors;
    private final Clock clock;
    private final TransactionLogCatchUpFactory transactionLogCatchUpFactory;
    private final JobScheduler jobScheduler;

    /**
     * Dependencies that can be resolved immediately after launching the backup tool
     *
     * @param outsideWorld filesystem and output streams that the tool interacts with
     * @param logProvider made available to subsequent dependency resolution classes
     * @param monitors will become shared across all resolved dependencies
     */
    BackupModule( OutsideWorld outsideWorld, LogProvider logProvider, Monitors monitors )
    {
        this.outsideWorld = outsideWorld;
        this.logProvider = logProvider;
        this.monitors = monitors;
        this.clock = Clock.systemDefaultZone();
        this.transactionLogCatchUpFactory = new TransactionLogCatchUpFactory();
        this.fileSystemAbstraction = outsideWorld.fileSystem();
        this.jobScheduler = createInitialisedScheduler();
    }

    public LogProvider getLogProvider()
    {
        return logProvider;
    }

    public FileSystemAbstraction getFileSystemAbstraction()
    {
        return fileSystemAbstraction;
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

    public OutsideWorld getOutsideWorld()
    {
        return outsideWorld;
    }

    public JobScheduler jobScheduler()
    {
        return jobScheduler;
    }
}
