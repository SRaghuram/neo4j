/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.config;

import com.google.common.collect.Lists;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class Neo4jArchive
{
    private static final String JVM_ARG_KEY = "dbms.jvm.additional";
    private static final String DEFAULT_JVM_ARGS_FILENAME = "neo4j.conf";

    private final List<String> jvmArgs;

    public static String[] extractJvmArgsFrom( File neo4jPackage )
    {
        Neo4jArchive neo4jArchive = new Neo4jArchive( neo4jPackage );
        if ( !neo4jArchive.hasDefaultJvmArgs() )
        {
            throw new RuntimeException(
                    format( "No JVM args found in package: %s", neo4jPackage.getAbsolutePath() ) );
        }
        return neo4jArchive.defaultJvmArgs().stream().toArray( String[]::new );
    }

    public Neo4jArchive( File archive )
    {
        try ( InputStream archiveInputStream = new FileInputStream( archive ) )
        {
            List<String> neo4jConfigLines = extractNeo4jConfigLines( archiveInputStream );
            this.jvmArgs = (null == neo4jConfigLines) ? null : filterJvmArgs( neo4jConfigLines );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException(
                    format( "Error opening archive: %s", archive.getAbsolutePath() ), e );
        }
    }

    public Neo4jArchive( InputStream archive )
    {
        List<String> neo4jConfigLines = extractNeo4jConfigLines( archive );
        this.jvmArgs = (null == neo4jConfigLines) ? null : filterJvmArgs( neo4jConfigLines );
    }

    public boolean hasDefaultJvmArgs()
    {
        return null != jvmArgs;
    }

    public List<String> defaultJvmArgs()
    {
        return (null == jvmArgs) ? Lists.newArrayList() : jvmArgs;
    }

    private static List<String> filterJvmArgs( List<String> neo4jConfigLines )
    {
        return neo4jConfigLines.stream()
                .filter( line -> line.startsWith( JVM_ARG_KEY ) )
                .map( line -> line.substring( line.indexOf( "=" ) + 1, line.length() ) )
                .collect( toList() );
    }

    private List<String> extractNeo4jConfigLines( InputStream archive )
    {
        try ( GZIPInputStream uncompressed = new GZIPInputStream( archive );
              TarArchiveInputStream extracted = new TarArchiveInputStream( uncompressed ) )
        {
            TarArchiveEntry tarArchiveEntry;
            while ( null != (tarArchiveEntry = extracted.getNextTarEntry()) )
            {
                if ( tarArchiveEntry.getName().contains( DEFAULT_JVM_ARGS_FILENAME ) )
                {
                    BufferedReader br = new BufferedReader( new InputStreamReader( extracted ) );
                    return br.lines().collect( toList() );
                }
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( format( "Error trying to extract %s", DEFAULT_JVM_ARGS_FILENAME ), e );
        }
        return null;
    }
}
