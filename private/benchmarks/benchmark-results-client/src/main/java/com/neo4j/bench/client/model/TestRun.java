/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.model;

import com.neo4j.bench.client.util.S3Util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.neo4j.driver.v1.Value;

import static com.neo4j.bench.client.ClientUtil.generateUniqueId;

import static java.util.Objects.requireNonNull;

public class TestRun
{
    private static final String ID = "id";
    private static final String DURATION = "duration";
    private static final String DATE = "date";
    private static final String BUILD = "build";
    private static final String ARCHIVE = "archive";
    private static final String PARENT_BUILD = "parent_build";
    private static final String TRIGGERED_BY = "triggered_by";

    private final String id;
    private final String triggeredBy;
    private final long parentBuild;
    private final long durationMs;
    private final long dateUtc;
    private final long build;
    private String archive;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public TestRun()
    {
        this( "-1", -1, -1, -1, -1, "-1" );
    }

    public TestRun( Value value )
    {
        this(
                value.get( ID ).asString(),
                value.get( DURATION ).asLong(),
                value.get( DATE ).asLong(),
                value.get( BUILD ).asLong(),
                value.get( PARENT_BUILD ).asLong(),
                value.get( TRIGGERED_BY ).asString() );
    }

    public TestRun( long durationMs, long dateUtc, long build, long parentBuild, String triggeredBy )
    {
        this( generateUniqueId(), durationMs, dateUtc, build, parentBuild, triggeredBy );
    }

    public TestRun( String id, long durationMs, long dateUtc, long build, long parentBuild, String triggeredBy )
    {
        this.id = requireNonNull( id );
        this.durationMs = durationMs;
        this.dateUtc = dateUtc;
        this.build = build;
        this.parentBuild = parentBuild;
        this.triggeredBy = requireNonNull( triggeredBy );
    }

    public String id()
    {
        return id;
    }

    public long durationMs()
    {
        return durationMs;
    }

    public long dateUtc()
    {
        return dateUtc;
    }

    public long build()
    {
        return build;
    }

    public String archive()
    {
        return archive;
    }

    public void setArchive( String archive ) throws IOException
    {
        S3Util.assertSaneS3Path( archive );
        this.archive = archive;
    }

    public Map<String,Object> toMap()
    {
        Map<String,Object> map = new HashMap<>();
        map.put( ID, id );
        map.put( DURATION, durationMs );
        map.put( DATE, dateUtc );
        map.put( BUILD, build );
        if ( null != archive )
        {
            map.put( ARCHIVE, archive );
        }
        map.put( PARENT_BUILD, parentBuild );
        map.put( TRIGGERED_BY, triggeredBy );
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
        TestRun testRun = (TestRun) o;
        return durationMs == testRun.durationMs &&
               dateUtc == testRun.dateUtc &&
               build == testRun.build &&
               Objects.equals( id, testRun.id ) &&
               parentBuild == testRun.parentBuild &&
               Objects.equals( archive, testRun.archive );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( id, durationMs, dateUtc, build, archive, parentBuild );
    }

    @Override
    public String toString()
    {
        return "TestRun{" +
               "id='" + id + '\'' +
               ", durationMs=" + durationMs +
               ", dateUtc=" + dateUtc +
               ", build=" + build +
               ", archive='" + archive + '\'' +
               ", parentBuild='" + parentBuild + '\'' +
               '}';
    }
}
