/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.commandline.dbms;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.Args;
import org.neo4j.io.layout.DatabaseLayout;

class ImporterFactory
{
    Importer getImporterForMode( String mode, Args parsedArgs, Config config, OutsideWorld outsideWorld, DatabaseLayout databaseLayout )
            throws IncorrectUsage, CommandFailed
    {
        Importer importer;
        switch ( mode )
        {
        case "database":
            importer = new DatabaseImporter( parsedArgs, databaseLayout );
            break;
        case "csv":
            importer = new CsvImporter( parsedArgs, config, outsideWorld, databaseLayout );
            break;
        default:
            throw new CommandFailed( "Invalid mode specified." );
        }
        return importer;
    }
}
