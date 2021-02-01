/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.vmstat;

import com.neo4j.bench.common.process.ProcessWrapper;
import com.neo4j.bench.common.profiling.ExternalProfiler;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.RecordingDescriptor;
import com.neo4j.bench.common.profiling.metrics.Chart;
import com.neo4j.bench.common.profiling.metrics.ChartWriter;
import com.neo4j.bench.common.profiling.metrics.Point;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.JvmVersion;
import com.neo4j.bench.common.util.PathUtil;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.model.profiling.RecordingType;

import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class VmStatTracer implements ExternalProfiler
{
    private ProcessWrapper vmstat;

    @Override
    public List<String> invokeArgs( ForkDirectory forkDirectory,
                                    ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        return Collections.emptyList();
    }

    @Override
    public JvmArgs jvmArgs( JvmVersion jvmVersion,
                            ForkDirectory forkDirectory,
                            ProfilerRecordingDescriptor profilerRecordingDescriptor,
                            Resources resources )
    {
        return JvmArgs.empty();
    }

    @Override
    public void beforeProcess( ForkDirectory forkDirectory,
                               ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        RecordingDescriptor recordingDescriptor = profilerRecordingDescriptor.recordingDescriptorFor( RecordingType.TRACE_VMSTAT_CHART );
        Path vmstatLog = forkDirectory.findOrCreate( vmStatOutputLogName( recordingDescriptor.additionalParams() ) );
        vmstat = ProcessWrapper.start( new ProcessBuilder()
                                               .command( "vmstat", "2", "--timestamp", "--wide", "--unit", "M", "--one-header" )
                                               .redirectOutput( vmstatLog.toFile() )
                                               .redirectError( Redirect.INHERIT ) );
    }

    @Override
    public void afterProcess( ForkDirectory forkDirectory,
                              ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        vmstat.stop();
        RecordingDescriptor recordingDescriptor = profilerRecordingDescriptor.recordingDescriptorFor( RecordingType.TRACE_VMSTAT_CHART );
        Path vmStatLog = forkDirectory.findOrCreate( vmStatOutputLogName( recordingDescriptor.additionalParams() ) );
        Path targetJson = forkDirectory.registerPathFor( recordingDescriptor );
        Map<String,List<Point>> data = VmStatReader.read( vmStatLog );
        List<Chart> charts = VmStatChartCreator.createCharts( data );
        ChartWriter.write( charts, targetJson );
    }

    private static String vmStatOutputLogName( Parameters parameters )
    {
        String additionalParametersString = parameters.isEmpty() ? "" : "-" + parameters.toString();
        return PathUtil.withDefaultMaxLength().limitLength( "vmstat", additionalParametersString, ".log" );
    }

    @Override
    public void processFailed( ForkDirectory forkDirectory,
                               ProfilerRecordingDescriptor profilerRecordingDescriptor )
    {
        // do nothing
    }
}
