/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.model;

import com.neo4j.bench.client.profiling.RecordingType;
import com.neo4j.bench.client.util.S3Util;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ProfilerRecordings
{
    private final String jfr;
    private final String jfrFlamegraph;
    private final String async;
    private final String asyncFlamegraph;
    private final String gcLog;
    private final String gcSummary;
    private final String gcCsv;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public ProfilerRecordings()
    {
        this( "", "", "", "", "", "", "" );
    }

    private ProfilerRecordings(
            String jfr,
            String jfrFlamegraph,
            String async,
            String asyncFlamegraph,
            String gcLog,
            String gcSummary,
            String gcCsv )
    {
        this.jfr = jfr;
        this.jfrFlamegraph = jfrFlamegraph;
        this.async = async;
        this.asyncFlamegraph = asyncFlamegraph;
        this.gcLog = gcLog;
        this.gcSummary = gcSummary;
        this.gcCsv = gcCsv;
        // sanity check, to make sure the same names (paths) are never assigned to multiple properties
        assertNoDuplicatePaths();
    }

    public ProfilerRecordings with( RecordingType property, String path )
    {
        // S3 path should be of the form <bucket>/<remainder>, not s3://<bucket>/<remainder>
        S3Util.assertSaneS3Path( path );
        switch ( property )
        {
        case JFR:
            return new ProfilerRecordings( path, jfrFlamegraph, async, asyncFlamegraph, gcLog, gcSummary, gcCsv );
        case ASYNC:
            return new ProfilerRecordings( jfr, jfrFlamegraph, path, asyncFlamegraph, gcLog, gcSummary, gcCsv );
        case JFR_FLAMEGRAPH:
            return new ProfilerRecordings( jfr, path, async, asyncFlamegraph, gcLog, gcSummary, gcCsv );
        case ASYNC_FLAMEGRAPH:
            return new ProfilerRecordings( jfr, jfrFlamegraph, async, path, gcLog, gcSummary, gcCsv );
        case GC_LOG:
            return new ProfilerRecordings( jfr, jfrFlamegraph, async, asyncFlamegraph, path, gcSummary, gcCsv );
        case GC_SUMMARY:
            return new ProfilerRecordings( jfr, jfrFlamegraph, async, asyncFlamegraph, gcLog, path, gcCsv );
        case GC_CSV:
            return new ProfilerRecordings( jfr, jfrFlamegraph, async, asyncFlamegraph, gcLog, gcSummary, path );
        default:
            throw new RuntimeException( "Unrecognized property name: " + property );
        }
    }

    private void assertNoDuplicatePaths()
    {
        Map<String,String> map = toMap();
        if ( map.size() != map.values().stream().distinct().count() )
        {
            throw new RuntimeException( "Found duplicate paths in profile: " + map.toString() );
        }
    }

    public Map<String,String> toMap()
    {
        Map<String,String> map = new HashMap<>();
        if ( null != jfr && !jfr.isEmpty() )
        {
            map.put( RecordingType.JFR.propertyKey(), jfr );
        }
        if ( null != async && !async.isEmpty() )
        {
            map.put( RecordingType.ASYNC.propertyKey(), async );
        }
        if ( null != jfrFlamegraph && !jfrFlamegraph.isEmpty() )
        {
            map.put( RecordingType.JFR_FLAMEGRAPH.propertyKey(), jfrFlamegraph );
        }
        if ( null != asyncFlamegraph && !asyncFlamegraph.isEmpty() )
        {
            map.put( RecordingType.ASYNC_FLAMEGRAPH.propertyKey(), asyncFlamegraph );
        }
        if ( null != gcLog && !gcLog.isEmpty() )
        {
            map.put( RecordingType.GC_LOG.propertyKey(), gcLog );
        }
        if ( null != gcSummary && !gcSummary.isEmpty() )
        {
            map.put( RecordingType.GC_SUMMARY.propertyKey(), gcSummary );
        }
        if ( null != gcCsv && !gcCsv.isEmpty() )
        {
            map.put( RecordingType.GC_CSV.propertyKey(), gcCsv );
        }
        return map;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        ProfilerRecordings profilerRecordings = (ProfilerRecordings) o;
        return Objects.equals( jfr, profilerRecordings.jfr ) &&
               Objects.equals( jfrFlamegraph, profilerRecordings.jfrFlamegraph ) &&
               Objects.equals( async, profilerRecordings.async ) &&
               Objects.equals( asyncFlamegraph, profilerRecordings.asyncFlamegraph ) &&
               Objects.equals( gcLog, profilerRecordings.gcLog ) &&
               Objects.equals( gcSummary, profilerRecordings.gcSummary ) &&
               Objects.equals( gcCsv, profilerRecordings.gcCsv );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( jfr, jfrFlamegraph, async, asyncFlamegraph, gcLog, gcSummary, gcCsv );
    }

    @Override
    public String toString()
    {
        return "Profiles{" +
               "jfr='" + jfr + '\'' +
               ", jfrFlamegraph='" + jfrFlamegraph + '\'' +
               ", async='" + async + '\'' +
               ", asyncFlamegraph='" + asyncFlamegraph + '\'' +
               ", gcLog='" + gcLog + '\'' +
               ", gcSummary='" + gcSummary + '\'' +
               ", gcCsv='" + gcCsv + '\'' +
               '}';
    }
}
