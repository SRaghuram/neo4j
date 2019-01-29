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

import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.csv.Decorator;

public class AdditiveLabelFromColumnDecorator implements Decorator<InputNode>
{
    private final int columnIndex;

    public AdditiveLabelFromColumnDecorator( int columnIndex )
    {
        this.columnIndex = (columnIndex * 2) + 1;
    }

    @Override
    public InputNode apply( InputNode inputNode ) throws RuntimeException
    {
        // persons: a|b|c|...
        //         [a, _, b, _, c, _,...]
        //         [0, 1, 2, 3, 4, 5,...]
        String labelString = (String) inputNode.properties()[columnIndex];
        labelString =
                labelString.substring( 0, 1 ).toUpperCase() + labelString.substring( 1, labelString.length() );
        String[] newLabels = new String[inputNode.labels().length + 1];
        newLabels[newLabels.length - 1] = labelString;

        final int originalPropertiesLength = inputNode.properties().length;
        Object[] newProperties = new Object[originalPropertiesLength - 2];
        int newIndex = 0;
        for ( int i = 0; i < originalPropertiesLength; i++ )
        {
            if ( i != columnIndex - 1 && i != columnIndex )
            {
                newProperties[newIndex] = inputNode.properties()[i];
                newIndex++;
            }
        }

        Long newFirstPropertyId;
        try
        {
            newFirstPropertyId = inputNode.firstPropertyId();
        }
        catch ( NullPointerException e )
        {
            newFirstPropertyId = null;
        }

        return new InputNode(
                inputNode.sourceDescription(),
                inputNode.lineNumber(),
                inputNode.position(),
                inputNode.group(),
                inputNode.id(),
                newProperties,
                newFirstPropertyId,
                newLabels,
                inputNode.labelField()
        );
    }
}
