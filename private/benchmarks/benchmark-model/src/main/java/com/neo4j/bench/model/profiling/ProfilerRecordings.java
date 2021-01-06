/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.profiling;

import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.util.MapPrinter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ProfilerRecordings
{
    // NOTE: could use one map of maps here, with parameters as key of inner maps, but JSON (de)serializing needs to be updated for that
    private final Map<RecordingType,List<Parameters>> recordingParameters;
    private final Map<RecordingType,List<String>> recordingFilenames;

    /**
     * WARNING: Never call this explicitly. No-params constructor is only used for JSON (de)serialization.
     */
    public ProfilerRecordings()
    {
        this( new HashMap<>(), new HashMap<>() );
    }

    public ProfilerRecordings with( RecordingType recordingType, Parameters parameters, String path )
    {
        // S3 path should be of the form <bucket>/<remainder>, not s3://<bucket>/<remainder>
        assertSaneS3Path( path );

        List<String> filenamesList = recordingFilenames.computeIfAbsent( recordingType, r -> new ArrayList<>() );
        List<Parameters> parametersList = recordingParameters.computeIfAbsent( recordingType, r -> new ArrayList<>() );

        if ( parametersList.contains( parameters ) )
        {
            String oldFilename = filenamesList.get( parametersList.indexOf( parameters ) );
            throw new RuntimeException( "Duplicate entry!\n" +
                                        "Recording:  " + recordingType + "\"" +
                                        "Parameters: '" + parameters.toString() + "'\n" +
                                        "Old path:   '" + oldFilename + "'\n" +
                                        "New path:   '" + path + "'" );
        }

        filenamesList.add( path );
        parametersList.add( parameters );
        return this;
    }

    private ProfilerRecordings( Map<RecordingType,List<Parameters>> recordingParameters, Map<RecordingType,List<String>> recordingFilenames )
    {
        this.recordingParameters = recordingParameters;
        this.recordingFilenames = recordingFilenames;
        // sanity check, to make sure the same names (paths) are never assigned to multiple properties
        assertNoDuplicatePaths( toMap( recordingFilenames, recordingParameters ) );
    }

    private static Map<String,String> toMap( Map<RecordingType,List<String>> recordingFilenames,
                                             Map<RecordingType,List<Parameters>> recordingParameters )
    {
        Map<String,String> map = new HashMap<>();
        for ( var entry : recordingFilenames.entrySet() )
        {
            RecordingType recordingType = entry.getKey();
            List<String> filenameList = entry.getValue();
            List<Parameters> parametersList = recordingParameters.get( recordingType );
            for ( int i = 0; i < filenameList.size(); i++ )
            {
                String filename = filenameList.get( i );
                Parameters parameters = parametersList.get( i );
                String propertyKey = profilesPropertyKeyFor( recordingType, parameters );
                map.put( propertyKey, filename );
            }
        }
        return map;
    }

    public static String profilesPropertyKeyFor( RecordingType recordingType, Parameters parameters )
    {
        return parameters.isEmpty()
               ? recordingType.propertyKey()
               : recordingType.propertyKey() + "_" + parameters.toString();
    }

    private static void assertNoDuplicatePaths( Map<String,String> map )
    {
        if ( map.size() != map.values().stream().distinct().count() )
        {
            throw new RuntimeException( "Found duplicate paths in profile: " + map.toString() );
        }
    }

    public Map<String,String> toMap()
    {
        return toMap( recordingFilenames, recordingParameters );
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
        ProfilerRecordings that = (ProfilerRecordings) o;
        return Objects.equals( recordingParameters, that.recordingParameters ) &&
               Objects.equals( recordingFilenames, that.recordingFilenames );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( recordingParameters, recordingFilenames );
    }

    @Override
    public String toString()
    {
        return MapPrinter.prettyPrint( toMap() );
    }

    public static void assertSaneS3Path( String... s3Paths )
    {
        for ( String s3Path : s3Paths )
        {
            if ( s3Path.startsWith( "http://" ) || s3Path.startsWith( "s3://" ) )
            {
                throw new RuntimeException( "S3 path should not include protocol: " + s3Path );
            }
            if ( !s3Path.contains( "/" ) )
            {
                throw new RuntimeException( "S3 path must begin with '<bucket>/': " + s3Path );
            }
        }
    }
}
