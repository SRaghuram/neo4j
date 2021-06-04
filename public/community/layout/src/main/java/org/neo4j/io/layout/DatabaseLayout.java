/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.io.layout;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.DbmsReadOnlyChecker;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.io.fs.FileUtils;

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
    private static final String BACKUP_TOOLS_FOLDER = "tools";

    private Path databaseDirectory;
    private final Neo4jLayout neo4jLayout;
    private final String databaseName;

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

    static DatabaseLayout of( Neo4jLayout neo4jLayout, String databaseName, Config config )
    {
        return new DatabaseLayout( neo4jLayout, databaseName, config, null );
    }

    static DatabaseLayout of( Neo4jLayout neo4jLayout, String databaseName, Config config, DbmsReadOnlyChecker dbmsReadOnlyChecker )
    {
        return new DatabaseLayout( neo4jLayout, databaseName, config, dbmsReadOnlyChecker );
    }
    public String[] storeTypes = null;
    Config config;
    static String readonlySuffix = null;
    private DbmsReadOnlyChecker dbmsReadOnlyChecker;
    protected DatabaseLayout( Neo4jLayout neo4jLayout, String databaseName, Config config, DbmsReadOnlyChecker dbmsReadOnlyChecker )
    {
        this.config = config;
        this.dbmsReadOnlyChecker = dbmsReadOnlyChecker;
        readonlySuffix = config.get( GraphDatabaseInternalSettings.readonly_database_name_suffix );
        var normalizedName = new NormalizedDatabaseName( databaseName ).name();
        this.neo4jLayout = neo4jLayout;
        String dirPath = normalizedName;
        String[] dbParts = normalizedName.split("\\.");
        if (dbParts.length > 1) {
            if (normalizedName.endsWith(".gds") && dbParts.length <= 3)
            {
                if (dbParts.length == 2) {
                    dirPath = dbParts[dbParts.length - 2];
                    if (!(new File(neo4jLayout.databasesDirectory()+File.separator+dirPath)).exists())
                        dirPath = normalizedName;
                }
                else
                    // length == 3 - make the middle the directory
                    dirPath = dbParts[1];
            }
            else
                storeTypes = dbParts;
        }
        this.databaseDirectory = FileUtils.getCanonicalFile( neo4jLayout.databasesDirectory().resolve( dirPath ) );
        this.databaseName = normalizedName;
    }

    public String[] getExistingDBNames()
    {
        File file = new File(String.valueOf(neo4jLayout.databasesDirectory()));
        File[] files = file.listFiles();
        String[] dbNames = new String[files.length];
        for (int i = 0; i < files.length; i++)
            dbNames[i] = files[i].getName();
        return dbNames;
    }

    public boolean isDBDirectoryPresent(String name)
    {
        File dirfile = new File(String.valueOf(neo4jLayout.databasesDirectory()));
        File[] files = dirfile.listFiles();
        for (File file: files) {
            if (file.getName().equalsIgnoreCase(name))
                return true;
        }
        return false;
    }

    public boolean isReadOnlyDB()
    {
        if ( dbmsReadOnlyChecker != null)
            return dbmsReadOnlyChecker.isReadOnly( this.databaseName );
        return false;
        //return isReadOnlyDB( this.databaseName );
    }
    /*static public boolean isReadOnlyDB(String dbName)
    {
        String[] dbParts = dbName.split("\\.");
        if (dbParts.length == 1)
            return false;
        if (readonlySuffix == null)
            return false;
        String[] suffixes = readonlySuffix.split(";");
        for (String suffix: suffixes)
            if ( dbName.endsWith( "."+ suffix ))
                return true;
        return false;
    }*/


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

    public Path databaseDirectory()
    {
        return databaseDirectory;
    }

    public Path backupToolsFolder()
    {
        return databaseDirectory().resolve( BACKUP_TOOLS_FOLDER );
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
        return file( DatabaseFile.RELATIONSHIP_TYPE_SCAN_STORE.getName() );
    }

    public Path countStore()
    {
        return file( DatabaseFile.COUNTS_STORE.getName() );
    }

    public Path relationshipGroupDegreesStore()
    {
        return file( DatabaseFile.RELATIONSHIP_GROUP_DEGREES_STORE.getName() );
    }

    public Path propertyStringStore()
    {
        return file( DatabaseFile.PROPERTY_STRING_STORE.getName() );
    }

    public Path relationshipStore()
    {
        return file( DatabaseFile.RELATIONSHIP_STORE.getName() );
    }

    public Path propertyStore()
    {
        return file( DatabaseFile.PROPERTY_STORE.getName() );
    }

    public Path nodeStore()
    {
        return file( DatabaseFile.NODE_STORE.getName() );
    }

    public Path nodeLabelStore()
    {
        return file( DatabaseFile.NODE_LABEL_STORE.getName() );
    }

    public Path propertyArrayStore()
    {
        return file( DatabaseFile.PROPERTY_ARRAY_STORE.getName() );
    }

    public Path propertyKeyTokenStore()
    {
        return file( DatabaseFile.PROPERTY_KEY_TOKEN_STORE.getName() );
    }

    public Path propertyKeyTokenNamesStore()
    {
        return file( DatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE.getName() );
    }

    public Path relationshipTypeTokenStore()
    {
        return file( DatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE.getName() );
    }

    public Path relationshipTypeTokenNamesStore()
    {
        return file( DatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE.getName() );
    }

    public Path labelTokenStore()
    {
        return file( DatabaseFile.LABEL_TOKEN_STORE.getName() );
    }

    public Path schemaStore()
    {
        return file( DatabaseFile.SCHEMA_STORE.getName() );
    }

    public Path relationshipGroupStore()
    {
        return file( DatabaseFile.RELATIONSHIP_GROUP_STORE.getName() );
    }

    public Path labelTokenNamesStore()
    {
        return file( DatabaseFile.LABEL_TOKEN_NAMES_STORE.getName() );
    }

    public Path indexStatisticsStore()
    {
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
}
