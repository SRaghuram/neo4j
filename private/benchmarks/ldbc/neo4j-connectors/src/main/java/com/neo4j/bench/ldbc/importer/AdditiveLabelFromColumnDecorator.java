/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
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
