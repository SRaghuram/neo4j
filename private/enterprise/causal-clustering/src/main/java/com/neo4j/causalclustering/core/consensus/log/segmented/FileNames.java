/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import java.nio.file.Path;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;

/**
 * Deals with file names for the RAFT log. The files are named as
 *
 *   raft.log.0
 *   raft.log.1
 *   ...
 *   raft.log.23
 *   ...
 *
 * where the suffix represents the version, which is a strictly monotonic sequence.
 */
public class FileNames
{
    static final String BASE_FILE_NAME = "raft.log.";
    private static final String VERSION_MATCH = "(0|[1-9]\\d*)";

    private final Path baseDirectory;
    private final Pattern logFilePattern;

    /**
     * Creates an object useful for managing RAFT log file names.
     *
     * @param baseDirectory The base directory in which the RAFT log files reside.
     */
    public FileNames( Path baseDirectory )
    {
        this.baseDirectory = baseDirectory;
        this.logFilePattern = Pattern.compile( BASE_FILE_NAME + VERSION_MATCH );
    }

    /**
     * Creates a file object for the specific version.
     * @param version The version.
     *
     * @return A file for the specific version.
     */
    Path getForSegment( long version )
    {
        return baseDirectory.resolve( BASE_FILE_NAME + version );
    }

    /**
     * Looks in the base directory for all suitable RAFT log files and returns a sorted map
     * with the version as key and File as value.
     *
     * @param fileSystem The filesystem.
     * @param log The message log.
     *
     * @return The sorted version to file map.
     */
    public SortedMap<Long,Path> getAllFiles( FileSystemAbstraction fileSystem, Log log )
    {
        SortedMap<Long,Path> versionFileMap = new TreeMap<>();

        for ( Path file : fileSystem.listFiles( baseDirectory ) )
        {
            Matcher matcher = logFilePattern.matcher( file.getFileName().toString() );

            if ( !matcher.matches() )
            {
                log.warn( "Found out of place file: " + file.getFileName() );
                continue;
            }

            versionFileMap.put( Long.valueOf( matcher.group( 1 ) ), file );
        }

        return versionFileMap;
    }
}
