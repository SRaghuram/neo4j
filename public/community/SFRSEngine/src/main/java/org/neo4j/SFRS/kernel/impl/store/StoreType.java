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
package org.neo4j.SFRS.kernel.impl.store;

import org.neo4j.internal.id.IdType;
import org.neo4j.io.layoutSFRS.DatabaseFile;
import org.neo4j.io.layoutSFRS.DatabaseLayout;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import java.util.Objects;
import java.util.Optional;

public enum StoreType
{
    NODE_LABEL( DatabaseFile.NODE_LABEL_STORE, IdType.NODE_LABELS )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createNodeLabelStore( cursorTracer );
                }
                @Override
                int fileSFRSStoreRootPage() {
                    return DatabaseLayout.NODE_LABEL_STORE_PAGE_OFFSET;
                }

                @Override
                boolean isExtensibleStore() {
                    return true;
                }
            },
    NODE( DatabaseFile.NODE_STORE, IdType.NODE )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createNodeStore( cursorTracer );
                }
                @Override
                int fileSFRSStoreRootPage() {
                    return DatabaseLayout.NODE_STORE_PAGE_OFFSET;
                }

                @Override
                boolean isExtensibleStore() {
                    return true;
                }
            },
    PROPERTY_KEY_TOKEN_NAME( DatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE, IdType.PROPERTY_KEY_TOKEN_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createPropertyKeyTokenNamesStore( cursorTracer );
                }
                @Override
                int fileSFRSStoreRootPage() {
                    return DatabaseLayout.PROPERTY_KEY_TOKEN_NAMES_STORE_PAGE_OFFSET;
                }

                @Override
                boolean isExtensibleStore() {
                    return false;
                }
            },
    PROPERTY_KEY_TOKEN( DatabaseFile.PROPERTY_KEY_TOKEN_STORE, IdType.PROPERTY_KEY_TOKEN )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createPropertyKeyTokenStore( cursorTracer );
                }
                @Override
                int fileSFRSStoreRootPage() {
                    return DatabaseLayout.PROPERTY_KEY_TOKEN_STORE_PAGE_OFFSET;
                }

                @Override
                boolean isExtensibleStore() {
                    return false;
                }
            },
    PROPERTY_STRING( DatabaseFile.PROPERTY_STRING_STORE, IdType.STRING_BLOCK )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createPropertyStringStore( cursorTracer );
                }
                @Override
                int fileSFRSStoreRootPage() {
                    return DatabaseLayout.PROPERTY_STRING_STORE_PAGE_OFFSET;
                }

                @Override
                boolean isExtensibleStore() {
                    return true;
                }
            },
    PROPERTY_ARRAY( DatabaseFile.PROPERTY_ARRAY_STORE, IdType.ARRAY_BLOCK )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createPropertyArrayStore( cursorTracer );
                }
                @Override
                int fileSFRSStoreRootPage() {
                    return DatabaseLayout.PROPERTY_ARRAY_STORE_PAGE_OFFSET;
                }

                @Override
                boolean isExtensibleStore() {
                    return true;
                }
            },
    PROPERTY( DatabaseFile.PROPERTY_STORE, IdType.PROPERTY )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createPropertyStore( cursorTracer );
                }
                @Override
                int fileSFRSStoreRootPage() {
                    return DatabaseLayout.PROPERTY_STORE_PAGE_OFFSET;
                }

                @Override
                boolean isExtensibleStore() {
                    return true;
                }
            },
    RELATIONSHIP( DatabaseFile.RELATIONSHIP_STORE, IdType.RELATIONSHIP )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createRelationshipStore( cursorTracer );
                }
                @Override
                int fileSFRSStoreRootPage() {
                    return DatabaseLayout.RELATIONSHIP_STORE_PAGE_OFFSET;
                }

                @Override
                boolean isExtensibleStore() {
                    return true;
                }
            },
    RELATIONSHIP_TYPE_TOKEN_NAME( DatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE, IdType.RELATIONSHIP_TYPE_TOKEN_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createRelationshipTypeTokenNamesStore( cursorTracer );
                }
                @Override
                int fileSFRSStoreRootPage() {
                    return DatabaseLayout.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_PAGE_OFFSET;
                }

                @Override
                boolean isExtensibleStore() {
                    return false;
                }
            },
    RELATIONSHIP_TYPE_TOKEN( DatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE, IdType.RELATIONSHIP_TYPE_TOKEN )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createRelationshipTypeTokenStore( cursorTracer );
                }
                @Override
                int fileSFRSStoreRootPage() {
                    return DatabaseLayout.RELATIONSHIP_TYPE_TOKEN_STORE_PAGE_OFFSET;
                }

                @Override
                boolean isExtensibleStore() {
                    return false;
                }
            },
    LABEL_TOKEN_NAME( DatabaseFile.LABEL_TOKEN_NAMES_STORE, IdType.LABEL_TOKEN_NAME )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createLabelTokenNamesStore( cursorTracer );
                }
                @Override
                int fileSFRSStoreRootPage() {
                    return DatabaseLayout.LABEL_TOKEN_NAMES_STORE_PAGE_OFFSET;
                }

                @Override
                boolean isExtensibleStore() {
                    return false;
                }
            },
    LABEL_TOKEN( DatabaseFile.LABEL_TOKEN_STORE, IdType.LABEL_TOKEN )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createLabelTokenStore( cursorTracer );
                }
                @Override
                int fileSFRSStoreRootPage() {
                    return DatabaseLayout.LABEL_TOKEN_STORE_PAGE_OFFSET;
                }

                @Override
                boolean isExtensibleStore() {
                    return false;
                }
            },
    SCHEMA( DatabaseFile.SCHEMA_STORE, IdType.SCHEMA )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createSchemaStore( cursorTracer );
                }
                @Override
                int fileSFRSStoreRootPage() {
                    return DatabaseLayout.SCHEMA_STORE_PAGE_OFFSET;
                }

                @Override
                boolean isExtensibleStore() {
                    return true;
                }
            },
    RELATIONSHIP_GROUP( DatabaseFile.RELATIONSHIP_GROUP_STORE, IdType.RELATIONSHIP_GROUP )
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createRelationshipGroupStore( cursorTracer );
                }
                @Override
                int fileSFRSStoreRootPage() {
                    return DatabaseLayout.RELATIONSHIP_GROUP_STORE_PAGE_OFFSET;
                }

                @Override
                boolean isExtensibleStore() {
                    return true;
                }
            },
    META_DATA( DatabaseFile.METADATA_STORE, IdType.NEOSTORE_BLOCK ) // Make sure this META store is last
            {
                @Override
                public CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer )
                {
                    return neoStores.createMetadataStore( cursorTracer );
                }
                @Override
                int fileSFRSStoreRootPage() {
                    return DatabaseLayout.METADATA_STORE_PAGE_OFFSET;
                }

                @Override
                boolean isExtensibleStore() {
                    return false;
                }
            };

    private final DatabaseFile databaseFile;
    private final IdType idType;

    StoreType( DatabaseFile databaseFile, IdType idType )
    {
        this.databaseFile = databaseFile;
        this.idType = idType;
    }

    abstract CommonAbstractStore open( NeoStores neoStores, PageCursorTracer cursorTracer );
    abstract int fileSFRSStoreRootPage();
    abstract boolean isExtensibleStore();

    public DatabaseFile getDatabaseFile()
    {
        return databaseFile;
    }

    public IdType getIdType()
    {
        return idType;
    }

    /**
     * Determine type of a store base on provided database file.
     *
     * @param databaseFile - database file to map
     * @return an {@link Optional} that wraps the matching store type of the specified file,
     * or {@link Optional#empty()} if the given file name does not match any store files.
     */
    public static Optional<StoreType> typeOf( DatabaseFile databaseFile )
    {
        Objects.requireNonNull( databaseFile );
        StoreType[] values = StoreType.values();
        for ( StoreType value : values )
        {
            if ( value.getDatabaseFile().equals( databaseFile ) )
            {
                return Optional.of( value );
            }
        }
        return Optional.empty();
    }
}
