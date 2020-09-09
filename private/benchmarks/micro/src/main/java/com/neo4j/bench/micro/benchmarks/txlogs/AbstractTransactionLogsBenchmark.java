/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.txlogs;

import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import org.neo4j.common.DependencyResolver;
import org.neo4j.io.fs.WritableChecksumChannel;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

@State( Scope.Benchmark )
public abstract class AbstractTransactionLogsBenchmark extends BaseDatabaseBenchmark
{
    LogFile logFile;
    WritableChecksumChannel channel;

    @Override
    public String benchmarkGroup()
    {
        return "Transaction logs";
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    @Override
    protected void afterDatabaseStart( DataGeneratorConfig config )
    {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) db()).getDependencyResolver();
        LogFiles logFiles = dependencyResolver.resolveDependency( LogFiles.class );
        logFile = logFiles.getLogFile();
        channel = logFile.getTransactionLogWriter().getChannel();
    }
}
