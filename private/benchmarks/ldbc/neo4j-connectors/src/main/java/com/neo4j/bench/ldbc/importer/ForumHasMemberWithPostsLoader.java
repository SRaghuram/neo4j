/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.ldbc.driver.csv.charseeker.BufferedCharSeeker;
import com.ldbc.driver.csv.charseeker.CharReadable;
import com.ldbc.driver.csv.charseeker.Extractors;
import com.ldbc.driver.csv.charseeker.Mark;
import com.ldbc.driver.csv.charseeker.Readables;
import com.ldbc.driver.csv.simple.SimpleCsvFileWriter;
import com.neo4j.bench.ldbc.Domain.HasMember;
import com.neo4j.bench.ldbc.cli.LdbcCli;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ldbc.driver.csv.charseeker.ThreadAheadReadable.threadAhead;
import static java.util.stream.Collectors.toList;

public class ForumHasMemberWithPostsLoader
{
    private static final char NO_ARRAY_SEPARATOR = ';';
    private static final char NO_TUPLE_SEPARATOR = ',';

    public static void createIn( File csvDataDir ) throws IOException
    {
        List<File> postsFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.POST.matcher( path.getFileName().toString() ).matches() )
                .map( Path::toFile )
                .collect( toList() );
        List<File> forumHasMemberPersonFiles = Files.list( csvDataDir.toPath() )
                .filter( path -> CsvFilesForMerge.FORUM_HAS_MEMBER_PERSON.matcher( path.getFileName().toString() )
                        .matches() )
                .map( Path::toFile )
                .collect( toList() );
        File forumHasMemberWithPostsPersonFile = new File(
                csvDataDir,
                CsvFilesForMerge.FORUM_HAS_MEMBER_WITH_POSTS_PERSON_FILENAME );
        if ( forumHasMemberWithPostsPersonFile.exists() )
        {
            FileUtils.forceDelete( forumHasMemberWithPostsPersonFile );
        }
        Map<Long,LongSet> forumMembersWithPosts = ForumHasMemberWithPostsLoader.load(
                postsFiles,
                LdbcCli.CHARSET,
                '|'
        );
        ForumHasMemberWithPostsLoader.write(
                forumHasMemberPersonFiles,
                forumHasMemberWithPostsPersonFile,
                '|',
                LdbcCli.CHARSET,
                forumMembersWithPosts
        );
    }

    // id:long|imageFile:string|creationDate:long|locationIP:string|browserUsed:string|language:string|content:string
    // |length:int|creator:long|forum:long|place:long|
    public static Map<Long,LongSet> load(
            List<File> mergedPostFiles,
            Charset charset,
            char columnDelimiter ) throws IOException
    {
        Map<Long,LongSet> forumMembersWithPosts = new HashMap<>();
        for ( File mergedPostFile : mergedPostFiles )
        {
            load( mergedPostFile, charset, columnDelimiter, forumMembersWithPosts );
        }
        return forumMembersWithPosts;
    }

    private static Map<Long,LongSet> load(
            File mergedPostFile,
            Charset charset,
            char columnDelimiter,
            Map<Long,LongSet> forumMembersWithPosts ) throws IOException
    {
        int bufferSize = 1 * 1024 * 1024;
        try ( InputStreamReader fileReader = new InputStreamReader( new FileInputStream( mergedPostFile ), charset ) )
        {
            try ( CharReadable threadAheadReader = threadAhead( Readables.wrap( fileReader ), bufferSize ) )
            {
                try ( BufferedCharSeeker mergePostCharSeeker = new BufferedCharSeeker( threadAheadReader, bufferSize ) )
                {
                    Extractors extractors = new Extractors( NO_ARRAY_SEPARATOR, NO_TUPLE_SEPARATOR );
                    Mark mark = new Mark();
                    int[] columnDelimiters = new int[]{columnDelimiter};

                    // skip headers
                    while ( !mark.isEndOfLine() )
                    {
                        mergePostCharSeeker.seek( mark, columnDelimiters );
                    }

                    // read: post.id
                    while ( mergePostCharSeeker.seek( mark, columnDelimiters ) )
                    {
                        // read: post.imageFile
                        mergePostCharSeeker.seek( mark, columnDelimiters );
                        // read: post.creationDate
                        mergePostCharSeeker.seek( mark, columnDelimiters );
                        // read: post.locationIp
                        mergePostCharSeeker.seek( mark, columnDelimiters );
                        // read: post.browserUsed
                        mergePostCharSeeker.seek( mark, columnDelimiters );
                        // read: post.language
                        mergePostCharSeeker.seek( mark, columnDelimiters );
                        // read: post.content
                        mergePostCharSeeker.seek( mark, columnDelimiters );
                        // read: post.length
                        mergePostCharSeeker.seek( mark, columnDelimiters );
                        // read: creator
                        mergePostCharSeeker.seek( mark, columnDelimiters );
                        long personId = mergePostCharSeeker.extract( mark, extractors.long_() ).longValue();
                        // read: forum
                        mergePostCharSeeker.seek( mark, columnDelimiters );
                        long forumId = mergePostCharSeeker.extract( mark, extractors.long_() ).longValue();
                        // read: place
                        mergePostCharSeeker.seek( mark, columnDelimiters );

                        LongSet members = forumMembersWithPosts.computeIfAbsent( forumId, k -> new LongOpenHashSet() );
                        members.add( personId );

                        // read to end of line, ignoring remaining columns <-- ignore end-of-row separator
                        while ( !mark.isEndOfLine() )
                        {
                            mergePostCharSeeker.seek( mark, columnDelimiters );
                        }
                    }
                    return forumMembersWithPosts;
                }
            }
        }
    }

    // Forum.id    Person.id   joinDate
    public static void write(
            List<File> forumHasMemberFiles,
            File forumHasMemberWithPostsFile,
            char columnDelimiter,
            Charset charset,
            Map<Long,LongSet> forumMembersWithPosts ) throws IOException
    {
        FileUtils.deleteQuietly( forumHasMemberWithPostsFile );
        com.ldbc.driver.util.FileUtils.tryCreateFile( forumHasMemberWithPostsFile, true );
        SimpleCsvFileWriter simpleCsvFileWriter =
                new SimpleCsvFileWriter( forumHasMemberWithPostsFile, Character.toString( columnDelimiter ) );
        String[] row = new String[3];
        row[0] = "Forum.id";
        row[1] = "Person.id";
        row[2] = HasMember.JOIN_DATE;
        simpleCsvFileWriter.writeRow( row );
        for ( File forumHasMemberFile : forumHasMemberFiles )
        {
            write(
                    forumHasMemberFile,
                    columnDelimiter,
                    charset,
                    forumMembersWithPosts,
                    simpleCsvFileWriter );
        }
        simpleCsvFileWriter.close();
    }

    // Forum.id    Person.id   joinDate
    private static void write(
            File forumHasMemberFile,
            char columnDelimiter,
            Charset charset,
            Map<Long,LongSet> forumMembersWithPosts,
            SimpleCsvFileWriter simpleCsvFileWriter ) throws IOException
    {
        String[] row = new String[3];

        int bufferSize = 1 * 1024 * 1024;

        try ( InputStreamReader fileReader =
                      new InputStreamReader( new FileInputStream( forumHasMemberFile ), charset ) )
        {
            try ( CharReadable threadAheadReader = threadAhead( Readables.wrap( fileReader ), bufferSize ) )
            {
                try ( BufferedCharSeeker forumHasMemberCharSeeker = new BufferedCharSeeker( threadAheadReader,
                        bufferSize ) )
                {
                    Extractors extractors = new Extractors( NO_ARRAY_SEPARATOR, NO_TUPLE_SEPARATOR );
                    Mark mark = new Mark();
                    int[] columnDelimiters = new int[]{columnDelimiter};

                    // skip headers
                    while ( !mark.isEndOfLine() )
                    {
                        forumHasMemberCharSeeker.seek( mark, columnDelimiters );
                    }

                    long forumId;
                    long personId;

                    // read: forum.id
                    while ( forumHasMemberCharSeeker.seek( mark, columnDelimiters ) )
                    {
                        forumId = forumHasMemberCharSeeker.extract( mark, extractors.long_() ).longValue();
                        LongSet members = forumMembersWithPosts.get( forumId );
                        if ( null != members )
                        {
                            // read: person.id
                            forumHasMemberCharSeeker.seek( mark, columnDelimiters );
                            personId = forumHasMemberCharSeeker.extract( mark, extractors.long_() ).longValue();
                            if ( members.contains( personId ) )
                            {
                                // read: forum.joinDate
                                forumHasMemberCharSeeker.seek( mark, columnDelimiters );
                                String joinDateString =
                                        forumHasMemberCharSeeker.extract( mark, extractors.string() ).value();
                                row[0] = Long.toString( forumId );
                                row[1] = Long.toString( personId );
                                row[2] = joinDateString;
                                simpleCsvFileWriter.writeRow( row );
                            }
                            else
                            {
                                // read: forum.joinDate
                                forumHasMemberCharSeeker.seek( mark, columnDelimiters );
                            }
                        }
                        else
                        {
                            // read: person.id
                            forumHasMemberCharSeeker.seek( mark, columnDelimiters );
                            // read: forum.joinDate
                            forumHasMemberCharSeeker.seek( mark, columnDelimiters );
                        }

                        // read to end of line, ignoring remaining columns <-- ignore end-of-row separator
                        while ( !mark.isEndOfLine() )
                        {
                            forumHasMemberCharSeeker.seek( mark, columnDelimiters );
                        }
                    }
                }
            }
        }
    }
}
