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
package org.neo4j.configuration.helpers;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;

import java.io.File;
import java.nio.file.Path;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_databases;
import static org.neo4j.configuration.GraphDatabaseSettings.writable_databases;

/**
 * Component for checking whether a database is configured to be read_only.
 */
@FunctionalInterface
public interface DbmsReadOnlyChecker
{
    static DbmsReadOnlyChecker writable()
    {
        return databaseName -> false;
    }

    static DbmsReadOnlyChecker readOnly()
    {
        return databaseName -> true;
    }

    boolean isReadOnly( String databaseName );

    default boolean isAliasDatabaseName( String databaseName )
    {
        return false;
    }

    class Default implements DbmsReadOnlyChecker
    {
        private final Config config;
        private Path dbDir = null;

        public Default( Config config )
        {
            this.config = config;
        }

        public Default( Config config, Path dbPath )
        {
            this.config = config;
            this.dbDir = dbPath;
        }

        @Override
        public boolean isReadOnly( String databaseName )
        {
            return check( config, databaseName );
        }

        private boolean check( Config config, String databaseName )
        {
            //System database can't be read only
            if ( SYSTEM_DATABASE_NAME.equals( databaseName ) )
            {
                return false;
            }
            return config.get( read_only_databases ).contains( databaseName ) ||
                    defaultsToReadOnlyAndNotWritable( config, databaseName ) || bySuffixInName ( config, databaseName );
        }

        @Override
        public boolean isAliasDatabaseName( String databaseName )
        {
            return bySuffixInName ( config, databaseName );
        }

        public boolean bySuffixInName( Config config, String name)
        {
            String[] dbParts = name.split("\\.");
            if (dbParts.length == 1)
                return false;
            String lastSuffix = dbParts[ dbParts.length -1 ];
            String dbName = dbParts[ dbParts.length -2 ];
            boolean bySuffixInName = false;
            String readonlySuffix = config.get( GraphDatabaseInternalSettings.readonly_database_name_suffix );
            if (readonlySuffix != null) {
                String[] suffixes = readonlySuffix.split(";");
                for (String suffix : suffixes)
                    if (lastSuffix.equalsIgnoreCase( suffix )) {
                        bySuffixInName = true;
                        break;
                    }
            }

            if (dbDir != null)
            {
                if (!(new File(dbDir+File.separator+ dbName)).exists())
                   return false;
            }
            return bySuffixInName;
        }
        private static boolean defaultsToReadOnlyAndNotWritable( Config config, String databaseName )
        {
            return config.get( read_only_database_default ) && !config.get( writable_databases ).contains( databaseName );
        }
    }
}
