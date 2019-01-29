/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
 */

package com.neo4j.bench.client.profiling;

import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.process.ProcessWrapper;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.JvmVersion;

import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static com.neo4j.bench.client.results.RunPhase.MEASUREMENT;

public class VmStatTracer implements ExternalProfiler
{
    private ProcessWrapper vmstat;

    @Override
    public List<String> jvmInvokeArgs( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        return Collections.emptyList();
    }

    @Override
    public List<String> jvmArgs( JvmVersion jvmVersion, ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        return Collections.emptyList();
    }

    @Override
    public void beforeProcess( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        ProfilerRecordingDescriptor recordingDescriptor = new ProfilerRecordingDescriptor( benchmarkGroup, benchmark, MEASUREMENT, ProfilerType.VM_STAT );

        Path vmstatLog = forkDirectory.pathFor( recordingDescriptor.filename( RecordingType.TRACE_VMSTAT ) );
        vmstat = ProcessWrapper.start( new ProcessBuilder()
                                               .command( "vmstat", "2", "-t", "-w", "-S", "M" )
                                               .redirectOutput( vmstatLog.toFile() )
                                               .redirectError( Redirect.INHERIT ) );
    }

    @Override
    public void afterProcess( ForkDirectory forkDirectory, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
    {
        vmstat.stop();
    }
}
