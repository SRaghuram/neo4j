/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.assist;

import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.ScheduledProfiler;
import com.neo4j.bench.common.profiling.ScheduledProfilerRunner;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.model.process.JvmArgs;

import java.util.List;

class ScheduledExternalProfilerAssist implements ExternalProfilerAssist
{
    private final ScheduledProfiler profiler;
    private final ExternalProfilerAssist delegate;
    private final ForkDirectory forkDirectory;
    private final ProfilerRecordingDescriptor descriptor;
    private final Jvm jvm;
    private ScheduledProfilerRunner scheduledProfilerRunner;

    ScheduledExternalProfilerAssist( ScheduledProfiler profiler,
                                     ExternalProfilerAssist delegate,
                                     ForkDirectory forkDirectory,
                                     ProfilerRecordingDescriptor descriptor,
                                     Jvm jvm )
    {
        this.profiler = profiler;
        this.delegate = delegate;
        this.forkDirectory = forkDirectory;
        this.descriptor = descriptor;
        this.jvm = jvm;
    }

    @Override
    public List<String> invokeArgs()
    {
        return delegate.invokeArgs();
    }

    @Override
    public JvmArgs jvmArgs()
    {
        return delegate.jvmArgs();
    }

    @Override
    public JvmArgs jvmArgsWithoutOOM()
    {
        return delegate.jvmArgsWithoutOOM();
    }

    @Override
    public void beforeProcess()
    {
        delegate.beforeProcess();
        scheduledProfilerRunner = new ScheduledProfilerRunner();
    }

    @Override
    public void afterProcess()
    {
        delegate.afterProcess();
        scheduledProfilerRunner.stop();
    }

    @Override
    public void processFailed()
    {
        delegate.processFailed();
        scheduledProfilerRunner.stop();
    }

    @Override
    public void schedule( Pid pid )
    {
        scheduledProfilerRunner.submit( profiler, forkDirectory, descriptor, jvm, pid );
    }
}
