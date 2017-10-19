/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.unsafe.impl.batchimport.restart;

import org.neo4j.csv.reader.Extractors;
import org.neo4j.unsafe.impl.batchimport.input.DataGeneratorInput;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.SimpleDataGenerator;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;

import static org.neo4j.unsafe.impl.batchimport.input.BadCollector.COLLECT_ALL;
import static org.neo4j.unsafe.impl.batchimport.input.BadCollector.UNLIMITED_TOLERANCE;
import static org.neo4j.unsafe.impl.batchimport.input.Collectors.silentBadCollector;
import static org.neo4j.unsafe.impl.batchimport.input.DataGeneratorInput.bareboneNodeHeader;
import static org.neo4j.unsafe.impl.batchimport.input.DataGeneratorInput.bareboneRelationshipHeader;

public class SimpleRandomizedInput
{
    public static Input randomizedInput( long seed, long nodeCount, long relationshipCount,
            float factorBadNodeData, float factorBadRelationshipData )
    {
        IdType idType = IdType.INTEGER;
        Extractors extractors = new Extractors( Configuration.COMMAS.arrayDelimiter() );
        SimpleDataGenerator generator = new SimpleDataGenerator(
                bareboneNodeHeader( idType, extractors ),
                bareboneRelationshipHeader( idType, extractors ),
                seed, nodeCount, 4, 4, idType, factorBadNodeData, factorBadRelationshipData );
        return new DataGeneratorInput( nodeCount, relationshipCount,
                generator.nodes(), generator.relationships(), idType, silentBadCollector( UNLIMITED_TOLERANCE, COLLECT_ALL ) );
    }
}
