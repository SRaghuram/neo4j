/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.database;

import com.neo4j.bench.common.util.BenchmarkUtil;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;

public class Neo4jStore extends Store
{
    private final Path homeDir;
    private final boolean isTemporaryCopy;
    private final Neo4jLayout layout;
    private final DatabaseName databaseName;
    private final boolean isFreki;

    private Neo4jStore( Path homeDir, DatabaseName databaseName, boolean isTemporaryCopy )
    {
        BenchmarkUtil.assertDirectoryExists( homeDir );
        this.homeDir = homeDir;
        this.databaseName = Objects.requireNonNull( databaseName );
        this.isTemporaryCopy = isTemporaryCopy;
        this.layout = Neo4jLayout.of( homeDir.toFile() );
        try
        {
            this.isFreki = Files.list( layout.databaseLayout( databaseName.name() ).databaseDirectory().toPath() )
                    .anyMatch( path -> path.getFileName().toString().startsWith( "main-store" ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public static Neo4jStore createFrom( Path originalTopLevelDir )
    {
        return new Neo4jStore( originalTopLevelDir, Neo4jDatabaseNames.defaultDatabase(), false );
    }

    public static Neo4jStore createFrom( Path originalTopLevelDir, DatabaseName databaseName )
    {
        return new Neo4jStore( originalTopLevelDir, databaseName, false );
    }

    public static Neo4jStore createTemporaryFrom( Path originalTopLevelDir )
    {
        return new Neo4jStore( originalTopLevelDir, Neo4jDatabaseNames.defaultDatabase(), true );
    }

    @Override
    public Neo4jStore makeTemporaryCopy()
    {
        return new Neo4jStore( StoreUtils.makeCopy( homeDir ), databaseName, true );
    }

    @Override
    public Neo4jStore makeCopyAt( Path topLevelDirCopy )
    {
        BenchmarkUtil.assertDoesNotExist( topLevelDirCopy );
        // we need to make sure topLevelDirCopy is not relative path
        BenchmarkUtil.assertDirectoryExists( topLevelDirCopy.toAbsolutePath().getParent() );
        StoreUtils.copy( homeDir, topLevelDirCopy );
        return Neo4jStore.createFrom( topLevelDirCopy, databaseName );
    }

    @Override
    public void assertDirectoryIsNeoStore()
    {
        BenchmarkUtil.assertDirectoryExists( homeDir );
        if ( !layout.databaseLayouts().isEmpty() )
        {
            throw new RuntimeException( "No database layouts found in: " + homeDir.toAbsolutePath() );
        }
    }

    @Override
    public Path topLevelDirectory()
    {
        return homeDir;
    }

    @Override
    public Path graphDbDirectory()
    {
        return layout.databaseLayout( databaseName.name() ).databaseDirectory().toPath();
    }

    @Override
    public DatabaseName databaseName()
    {
        return databaseName;
    }

    @Override
    public void removeTxLogs()
    {
        DatabaseLayout databaseLayout = layout.databaseLayout( databaseName.name() );
        Arrays.stream( Objects.requireNonNull( databaseLayout.getTransactionLogsDirectory().listFiles() ) )
              .forEach( File::delete );
    }

    @Override
    boolean isTemporaryCopy()
    {
        return isTemporaryCopy;
    }

    @Override
    public boolean isFreki()
    {
        return isFreki;
    }

    @Override
    public String toString()
    {
        return "Neo4jStore\n" +
               "\tHome : " + homeDir.toAbsolutePath() + "\n" +
               "\tDB   : " + databaseName + "\n" +
               "\tSize : " + BenchmarkUtil.bytesToString( bytes() );
    }
}
