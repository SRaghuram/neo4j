/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.assist;

import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.model.process.JvmArgs;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

class CompositeExternalProfilerAssist implements ExternalProfilerAssist
{
    private final List<ExternalProfilerAssist> profilers;

    CompositeExternalProfilerAssist( List<ExternalProfilerAssist> profilers )
    {
        this.profilers = profilers;
    }

    @Override
    public List<String> invokeArgs()
    {
        return profilers.stream()
                        .map( ExternalProfilerAssist::invokeArgs )
                        .flatMap( Collection::stream )
                        .distinct()
                        .collect( Collectors.toList() );
    }

    @Override
    public JvmArgs jvmArgs()
    {
        return profilers.stream()
                        .map( ExternalProfilerAssist::jvmArgs )
                        .reduce( JvmArgs.empty(), JvmArgs::merge );
    }

    @Override
    public JvmArgs jvmArgsWithoutOOM()
    {
        return profilers.stream()
                        .map( ExternalProfilerAssist::jvmArgsWithoutOOM )
                        .reduce( JvmArgs.empty(), JvmArgs::merge );
    }

    @Override
    public void beforeProcess()
    {
        profilers.forEach( ExternalProfilerAssist::beforeProcess );
    }

    @Override
    public void afterProcess()
    {
        profilers.forEach( ExternalProfilerAssist::afterProcess );
    }

    @Override
    public void processFailed()
    {
        profilers.forEach( ExternalProfilerAssist::processFailed );
    }

    @Override
    public void schedule( Pid pid )
    {
        profilers.forEach( profiler -> profiler.schedule( pid ) );
    }
}
