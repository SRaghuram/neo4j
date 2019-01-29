/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
 */

package com.neo4j.bench.ldbc.importer;

import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.csv.Decorator;

public class TagClassIsSubClassOfTagClassDecorator implements Decorator<InputRelationship>
{
    @Override
    public boolean isMutable()
    {
        return true;
    }

    @Override
    public InputRelationship apply( InputRelationship inputRelationship ) throws RuntimeException
    {
        // tags classes: id|name|url|isSubclassOf
        // NOTE: only pass through relationships that have both start and end nodes

        // id|id.val|name|name.val|url|url.val|isSubclassOf|isSubclassOf.val
        // 0 |1     |2   |3       |4  |5      |6           |7
        if ( null == inputRelationship.startNode() || null == inputRelationship.endNode() )
        {
            return new InputRelationship(
                    inputRelationship.sourceDescription(),
                    inputRelationship.lineNumber(),
                    inputRelationship.position(),
                    inputRelationship.properties(),
                    0L, // inputRelationship.firstPropertyId() causes NPE
                    -1,
                    -1,
                    inputRelationship.type(),
                    0 // inputRelationship.typeId() causes NPE
            );
        }
        else
        {
            return inputRelationship;
        }
    }
}
