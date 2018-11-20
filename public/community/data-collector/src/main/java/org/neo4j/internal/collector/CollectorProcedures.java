/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.internal.collector;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

@SuppressWarnings( "WeakerAccess" )
public class CollectorProcedures
{
    @Context
    public DataCollector dataCollector;

    @Description( "Retrieve statistical data about the current database." )
    @Procedure( name = "db.stats.retrieve", mode = Mode.READ )
    public Stream<RetrieveResult> retrieve( @Name( value = "section", defaultValue = "all" ) String section )
            throws InvalidArgumentsException
    {
        if ( section.toLowerCase().equals( GraphCountsSection.name.toLowerCase() ) )
        {
            return GraphCountsSection.collect( dataCollector.kernel );
        }
        throw new InvalidArgumentsException( String.format( "Unknown retrieve section '%s', known sections are ['%s']",
                                                            section, GraphCountsSection.name ) );
    }

    @Description( "Retrieve all available statistical data about the current database, in an anonymized form." )
    @Procedure( name = "db.stats.retrieveAllAnonymized", mode = Mode.READ )
    public Stream<RetrieveResult> retrieveAllAnonymized( @Name( value = "graphToken", defaultValue = "" ) String graphToken )
    {
        Map<String, Object> metaData = new HashMap<>();
        metaData.put( "graphToken", graphToken );
        metaData.put( "retrieveTime", ZonedDateTime.now() );
        Stream<RetrieveResult> meta = Stream.of( new RetrieveResult( "META", metaData ) );

        return Stream.concat( meta, GraphCountsSection.collect( dataCollector.kernel ) );
    }
}
