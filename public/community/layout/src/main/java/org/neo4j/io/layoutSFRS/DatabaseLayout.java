/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.io.layoutSFRS;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.io.fs.FileUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * File layout representation of the particular database. Facade for any kind of file lookup for a particular database storage implementation.
 * Any file retrieved from a layout can be considered a canonical file.
 * <br/>
 * No assumption should be made about where and how files of a particular database are positioned and all those details should be encapsulated inside.
 *
 * @see Neo4jLayout
 * @see DatabaseFile
 */
public class DatabaseLayout
{
    private static final String DATABASE_LOCK_FILENAME = "database_lock";

    private final Path databaseDirectory;
    private final Neo4jLayout neo4jLayout;
    private final String databaseName;
    private boolean isSFRSEngine = false;
    private boolean isOneIDFile = false;

    public static DatabaseLayout ofFlat( Path databaseDirectory )
    {
        Path canonical = FileUtils.getCanonicalFile( databaseDirectory );
        Path home = canonical.getParent();
        String dbName = canonical.getFileName().toString();
        return Neo4jLayout.ofFlat( home ).databaseLayout( dbName );
    }

    public static DatabaseLayout of( Config config )
    {
        return Neo4jLayout.of( config ).databaseLayout( config.get( GraphDatabaseSettings.default_database ) );
    }

    static DatabaseLayout of( Neo4jLayout neo4jLayout, String databaseName )
    {
        return new DatabaseLayout( neo4jLayout, databaseName );
    }

    protected DatabaseLayout( Neo4jLayout neo4jLayout, String databaseName )
    {
        var normalizedName = new NormalizedDatabaseName( databaseName ).name();
        this.neo4jLayout = neo4jLayout;
        this.databaseDirectory = FileUtils.getCanonicalFile( neo4jLayout.databasesDirectory().resolve( normalizedName ) );
        this.databaseName = normalizedName;
    }

    public Path getTransactionLogsDirectory()
    {
        return neo4jLayout.transactionLogsRootDirectory().resolve( getDatabaseName() );
    }

    public Path getScriptDirectory()
    {
        return neo4jLayout.scriptRootDirectory().resolve( getDatabaseName() );
    }

    public Path databaseLockFile()
    {
        return databaseDirectory().resolve( DATABASE_LOCK_FILENAME );
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public Neo4jLayout getNeo4jLayout()
    {
        return neo4jLayout;
    }

    public void setSFRSEngine( boolean value )
    {
        isSFRSEngine = value;
    }

    public void setIsOneIDFile( boolean value )
    {
        isOneIDFile = value;
    }

    public boolean isSFRSEngine()
    {
        return isSFRSEngine;
    }
    public boolean isOneIDFile()
    {
        return isOneIDFile;
    }

    private boolean checkSFRSEngine()
    {
        return isSFRSEngine();//((Config)storeLayout.getLayoutConfig()).isSFRSEngine(databaseName);
    }

    private Path forSFRSEngine()
    {
        return  file( org.neo4j.io.layout.DatabaseFile.METADATA_STORE.getName() );
    }

    public Path databaseDirectory()
    {
        return databaseDirectory;
    }

    public Path metadataStore()
    {
        return file( DatabaseFile.METADATA_STORE.getName() );
    }

    public Path labelScanStore()
    {
        return file( DatabaseFile.LABEL_SCAN_STORE.getName() );
    }

    public Path relationshipTypeScanStore()
    {
        if (checkSFRSEngine())
            return forSFRSEngine();
        return file( DatabaseFile.RELATIONSHIP_TYPE_SCAN_STORE.getName() );
    }

    public Path countStore()
    {
        return file( DatabaseFile.COUNTS_STORE.getName() );
    }

    public Path propertyStringStore()
    {
        if (checkSFRSEngine())
            return forSFRSEngine();
        return file( DatabaseFile.PROPERTY_STRING_STORE.getName() );
    }

    public Path relationshipStore()
    {
        if (checkSFRSEngine())
            return forSFRSEngine();
        return file( DatabaseFile.RELATIONSHIP_STORE.getName() );
    }

    public Path propertyStore()
    {
        if (checkSFRSEngine())
            return forSFRSEngine();
        return file( DatabaseFile.PROPERTY_STORE.getName() );
    }

    public Path nodeStore()
    {
        if (checkSFRSEngine())
            return forSFRSEngine();
        return file( DatabaseFile.NODE_STORE.getName() );
    }

    public Path nodeLabelStore()
    {
        if (checkSFRSEngine())
            return forSFRSEngine();
        return file( DatabaseFile.NODE_LABEL_STORE.getName() );
    }

    public Path propertyArrayStore()
    {
        if (checkSFRSEngine())
            return forSFRSEngine();
        return file( DatabaseFile.PROPERTY_ARRAY_STORE.getName() );
    }

    public Path propertyKeyTokenStore()
    {
        if (checkSFRSEngine())
            return forSFRSEngine();
        return file( DatabaseFile.PROPERTY_KEY_TOKEN_STORE.getName() );
    }

    public Path propertyKeyTokenNamesStore()
    {
        if (checkSFRSEngine())
            return forSFRSEngine();
        return file( DatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE.getName() );
    }

    public Path relationshipTypeTokenStore()
    {
        if (checkSFRSEngine())
            return forSFRSEngine();
        return file( DatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE.getName() );
    }

    public Path relationshipTypeTokenNamesStore()
    {
        if (checkSFRSEngine())
            return forSFRSEngine();
        return file( DatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE.getName() );
    }

    public Path labelTokenStore()
    {
        if (checkSFRSEngine())
            return forSFRSEngine();
        return file( DatabaseFile.LABEL_TOKEN_STORE.getName() );
    }

    public Path schemaStore()
    {
        if (checkSFRSEngine())
            return forSFRSEngine();
        return file( DatabaseFile.SCHEMA_STORE.getName() );
    }

    public Path relationshipGroupStore()
    {
        if (checkSFRSEngine())
            return forSFRSEngine();
        return file( DatabaseFile.RELATIONSHIP_GROUP_STORE.getName() );
    }

    public Path labelTokenNamesStore()
    {
        if (checkSFRSEngine())
            return forSFRSEngine();
        return file( DatabaseFile.LABEL_TOKEN_NAMES_STORE.getName() );
    }

    public Path indexStatisticsStore()
    {
        if (checkSFRSEngine())
            return forSFRSEngine();
        return file( DatabaseFile.INDEX_STATISTICS_STORE.getName() );
    }

    public Set<Path> idFiles()
    {
        return Arrays.stream( DatabaseFile.values() )
                .filter( DatabaseFile::hasIdFile )
                .flatMap( value -> idFile( value ).stream() )
                .collect( Collectors.toSet() );
    }

    public Set<Path> storeFiles()
    {
        return Arrays.stream( DatabaseFile.values() )
                .map( this::file )
                .collect( Collectors.toSet() );
    }

    public Optional<Path> idFile( DatabaseFile file )
    {
        if (checkSFRSEngine())
            return Optional.of(file( idFileName(org.neo4j.io.layout.DatabaseFile.METADATA_STORE.getName())));
        return file.hasIdFile() ? Optional.of( idFile( file.getName() ) ) : Optional.empty();
    }

    public Path file( String fileName )
    {
        return databaseDirectory.resolve( fileName );
    }

    public Path file( DatabaseFile databaseFile )
    {
        return file( databaseFile.getName() );
    }

    public Stream<Path> allFiles( DatabaseFile databaseFile )
    {
        return Stream.concat( idFile( databaseFile ).stream(), Stream.of( file( databaseFile ) ) );
    }

    public Path[] listDatabaseFiles( Predicate<? super Path> filter )
    {
        try ( Stream<Path> list = Files.list( databaseDirectory ) )
        {
            return list.filter( filter ).toArray( Path[]::new );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public Path idMetadataStore()
    {
        return idFile( DatabaseFile.METADATA_STORE.getName() );
    }

    public Path idNodeStore()
    {
        return idFile( DatabaseFile.NODE_STORE.getName() );
    }

    public Path idNodeLabelStore()
    {
        return idFile( DatabaseFile.NODE_LABEL_STORE.getName() );
    }

    public Path idPropertyStore()
    {
        return idFile( DatabaseFile.PROPERTY_STORE.getName() );
    }

    public Path idPropertyKeyTokenStore()
    {
        return idFile( DatabaseFile.PROPERTY_KEY_TOKEN_STORE.getName() );
    }

    public Path idPropertyKeyTokenNamesStore()
    {
        return idFile( DatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE.getName() );
    }

    public Path idPropertyStringStore()
    {
        return idFile( DatabaseFile.PROPERTY_STRING_STORE.getName() );
    }

    public Path idPropertyArrayStore()
    {
        return idFile( DatabaseFile.PROPERTY_ARRAY_STORE.getName() );
    }

    public Path idRelationshipStore()
    {
        return idFile( DatabaseFile.RELATIONSHIP_STORE.getName() );
    }

    public Path idRelationshipGroupStore()
    {
        return idFile( DatabaseFile.RELATIONSHIP_GROUP_STORE.getName() );
    }

    public Path idRelationshipTypeTokenStore()
    {
        return idFile( DatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE.getName() );
    }

    public Path idRelationshipTypeTokenNamesStore()
    {
        return idFile( DatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE.getName() );
    }

    public Path idLabelTokenStore()
    {
        return idFile( DatabaseFile.LABEL_TOKEN_STORE.getName() );
    }

    public Path idLabelTokenNamesStore()
    {
        return idFile( DatabaseFile.LABEL_TOKEN_NAMES_STORE.getName() );
    }

    public Path idSchemaStore()
    {
        return idFile( DatabaseFile.SCHEMA_STORE.getName() );
    }

    private Path idFile( String name )
    {
        if (checkSFRSEngine() || isOneIDFile())
            return file( idFileName(DatabaseFile.METADATA_STORE.getName()));
        return file( idFileName( name ) );
    }

    private static String idFileName( String storeName )
    {
        return storeName + ".id";
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseDirectory, neo4jLayout );
    }

    @Override
    public String toString()
    {
        return "DatabaseLayout{" + "databaseDirectory=" + databaseDirectory + ", transactionLogsDirectory=" + getTransactionLogsDirectory() + '}';
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
        DatabaseLayout that = (DatabaseLayout) o;
        return Objects.equals( databaseDirectory, that.databaseDirectory ) &&
               Objects.equals( neo4jLayout, that.neo4jLayout ) &&
               getTransactionLogsDirectory().equals( that.getTransactionLogsDirectory() );
    }

    public static int PAGEBLOCK_SIZE = 8;
    public static int PAGEBLOCK_HEADER_SIZE = 8;
    public static int PBTABLE_ENTRY_SIZE = 4;
    public static int ID_NUM_ENTRIES = 1024;
    public static int PBTABLE_INITIAL_OFFSET = 2 * PBTABLE_ENTRY_SIZE;
    public static int PBTABLE_HEADER_SIZE = 2 * PBTABLE_ENTRY_SIZE;
    public static int PBTABLE_PAGE_HEADER_SIZE = 0 * PBTABLE_ENTRY_SIZE;


    public static int METADATA_STORE_SIZE =  1;
    public static int PROPERTY_KEY_TOKEN_NAMES_STORE_SIZE =   1;
    public static int PROPERTY_KEY_TOKEN_STORE_SIZE =  1;
    public static int RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_SIZE =  1;
    public static int RELATIONSHIP_TYPE_TOKEN_STORE_SIZE =  1;
    public static int LABEL_TOKEN_NAMES_STORE_SIZE =  1;
    public static int LABEL_TOKEN_STORE_SIZE =  1;
    public static int SCHEMA_STORE_SIZE = PAGEBLOCK_SIZE;
    public static int NODE_LABEL_STORE_SIZE = PAGEBLOCK_SIZE;
    public static int NODE_STORE_SIZE = PAGEBLOCK_SIZE;
    public static int RELATIONSHIP_STORE_SIZE = PAGEBLOCK_SIZE;
    public static int RELATIONSHIP_GROUP_STORE_SIZE = PAGEBLOCK_SIZE;
    public static int PROPERTY_STORE_SIZE = PAGEBLOCK_SIZE;
    public static int PROPERTY_STRING_STORE_SIZE = PAGEBLOCK_SIZE;
    public static int PROPERTY_ARRAY_STORE_SIZE = PAGEBLOCK_SIZE;

    public static int METADATA_STORE_PAGE_OFFSET =  0;
    public static int PROPERTY_KEY_TOKEN_NAMES_STORE_PAGE_OFFSET =  METADATA_STORE_PAGE_OFFSET + METADATA_STORE_SIZE;
    public static int PROPERTY_KEY_TOKEN_STORE_PAGE_OFFSET =  PROPERTY_KEY_TOKEN_NAMES_STORE_PAGE_OFFSET + PROPERTY_KEY_TOKEN_NAMES_STORE_SIZE;
    public static int RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_PAGE_OFFSET =  PROPERTY_KEY_TOKEN_STORE_PAGE_OFFSET + PROPERTY_KEY_TOKEN_STORE_SIZE;
    public static int RELATIONSHIP_TYPE_TOKEN_STORE_PAGE_OFFSET =  RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_PAGE_OFFSET + RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_SIZE;
    public static int LABEL_TOKEN_NAMES_STORE_PAGE_OFFSET =  RELATIONSHIP_TYPE_TOKEN_STORE_PAGE_OFFSET + RELATIONSHIP_TYPE_TOKEN_STORE_SIZE;
    public static int LABEL_TOKEN_STORE_PAGE_OFFSET =  LABEL_TOKEN_NAMES_STORE_PAGE_OFFSET + LABEL_TOKEN_NAMES_STORE_SIZE;
    public static int SCHEMA_STORE_PAGE_OFFSET =  LABEL_TOKEN_STORE_PAGE_OFFSET + LABEL_TOKEN_STORE_SIZE;
    public static int NODE_LABEL_STORE_PAGE_OFFSET =  SCHEMA_STORE_PAGE_OFFSET + SCHEMA_STORE_SIZE;
    public static int NODE_STORE_PAGE_OFFSET =  NODE_LABEL_STORE_PAGE_OFFSET + NODE_LABEL_STORE_SIZE;
    public static int RELATIONSHIP_STORE_PAGE_OFFSET =  NODE_STORE_PAGE_OFFSET + NODE_STORE_SIZE;
    public static int RELATIONSHIP_GROUP_STORE_PAGE_OFFSET =  RELATIONSHIP_STORE_PAGE_OFFSET + RELATIONSHIP_STORE_SIZE;
    public static int PROPERTY_STORE_PAGE_OFFSET =  RELATIONSHIP_GROUP_STORE_PAGE_OFFSET + RELATIONSHIP_GROUP_STORE_SIZE;
    public static int PROPERTY_STRING_STORE_PAGE_OFFSET =  PROPERTY_STORE_PAGE_OFFSET + PROPERTY_STORE_SIZE;
    public static int PROPERTY_ARRAY_STORE_PAGE_OFFSET =  PROPERTY_STRING_STORE_PAGE_OFFSET + PROPERTY_STRING_STORE_SIZE;

    public static int MAX_INITIAL_PAGES = PROPERTY_ARRAY_STORE_PAGE_OFFSET + PROPERTY_ARRAY_STORE_SIZE;

    public static int nextAvailablePBId = PROPERTY_ARRAY_STORE_PAGE_OFFSET + PROPERTY_ARRAY_STORE_SIZE;


    public static synchronized int getNextPDBId()
    {
        int returnVal = nextAvailablePBId;
        nextAvailablePBId += PAGEBLOCK_SIZE;
        return returnVal;
    }

}
