/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.neo4j.dbms.archive.CompressionFormat;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PagedFile;

import static com.neo4j.kernel.impl.pagecache.PageCacheWarmer.SUFFIX_CACHEPROF;

final class Profile implements Comparable<Profile>
{
    static final String PROFILE_DIR = "profiles";
    private final Path baseDir;
    private final Path profileFile;
    private final Path pagedFile;
    private final long profileSequenceId;

    private Profile( Path baseDir, Path profileFile, Path pagedFile, long profileSequenceId )
    {
        Objects.requireNonNull( profileFile );
        Objects.requireNonNull( pagedFile );
        Objects.requireNonNull( baseDir );
        this.baseDir = baseDir;
        this.profileFile = profileFile;
        this.pagedFile = pagedFile;
        this.profileSequenceId = profileSequenceId;
    }

    @Override
    public int compareTo( Profile that )
    {
        int compare = pagedFile.compareTo( that.pagedFile );
        return compare == 0 ? Long.compare( profileSequenceId, that.profileSequenceId ) : compare;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o instanceof Profile )
        {
            Profile profile = (Profile) o;
            return profileFile.equals( profile.profileFile );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return profileFile.hashCode();
    }

    @Override
    public String toString()
    {
        return "Profile(" + profileSequenceId + " for " + pagedFile + ")";
    }

    Path file()
    {
        return profileFile;
    }

    void delete( FileSystemAbstraction fs )
    {
        fs.deleteFile( profileFile );
    }

    InputStream read( FileSystemAbstraction fs ) throws IOException
    {
        try
        {
            return CompressionFormat.decompress( () -> fs.openAsInputStream( profileFile ) );
        }
        catch ( IOException e )
        {
            throw new IOException( "Exception when building decompressor.", e );
        }
    }

    OutputStream write( FileSystemAbstraction fs ) throws IOException
    {
        // Create PROFILE_FOLDER if it does not exist.
        fs.mkdirs( profileFile.toFile().getParentFile().toPath() );
        try
        {
            return CompressionFormat.compress( () -> fs.openAsOutputStream( profileFile, true ), CompressionFormat.GZIP );
        }
        catch ( IOException e )
        {
            throw new IOException( "Exception when building compressor.", e );
        }
    }

    Profile next()
    {
        long next = profileSequenceId + 1L;
        return new Profile( baseDir, profileName( baseDir, pagedFile, next ), pagedFile, next );
    }

    static Profile first( Path databaseDirectory, Path file )
    {
        long profileSequenceId = 0;
        return new Profile( databaseDirectory, profileName( databaseDirectory, file, profileSequenceId ), file, profileSequenceId );
    }

    /**
     * Create profile file for mappedFile. It is assumed that baseDirectory contains mappedFile (can be in multiple sub directories down).
     * Profile file will be placed inside profileDirectory but keep it's sub directory structure relative to baseDirectory.
     */
    private static Path profileName( Path baseDirectory, Path mappedFile, long count )
    {
        Path profileDirectory = baseDirectory.resolve( PROFILE_DIR );
        Path profileFileDir = FileUtils.pathToFileAfterMove( baseDirectory, profileDirectory, mappedFile ).getParent();
        String name = mappedFile.getFileName().toString();
        return profileFileDir.resolve( name + "." + count + SUFFIX_CACHEPROF );
    }

    static Predicate<Profile> relevantTo( PagedFile pagedFile )
    {
        return p -> p.pagedFile.equals( pagedFile.path() );
    }

    static Stream<Profile> parseProfileName( Path basePath, Path profilePath, Path mappedFilePath )
    {
        String name = profilePath.getFileName().toString();
        if ( !name.endsWith( SUFFIX_CACHEPROF ) )
        {
            return Stream.empty();
        }
        int lastDot = name.lastIndexOf( '.' );
        int secondLastDot = name.lastIndexOf( '.', lastDot - 1 );
        String countStr = name.substring( secondLastDot + 1, lastDot );
        try
        {
            long sequenceId = Long.parseLong( countStr, 10 );
            String targetMappedFileName = name.substring( 0, secondLastDot );
            if ( targetMappedFileName.equals( mappedFilePath.getFileName().toString() ) )
            {
                return Stream.of( new Profile( basePath, profilePath, mappedFilePath, sequenceId ) );
            }
            else
            {
                return Stream.empty();
            }
        }
        catch ( NumberFormatException e )
        {
            return Stream.empty();
        }
    }
}
