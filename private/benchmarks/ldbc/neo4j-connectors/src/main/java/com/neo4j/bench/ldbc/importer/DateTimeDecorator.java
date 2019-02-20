/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.neo4j.bench.ldbc.connection.ImportDateUtil;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;

import java.text.ParseException;
import java.util.Calendar;

import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.csv.Decorator;

public class DateTimeDecorator<T extends InputEntity> implements Decorator<T>
{
    private final ImportDateUtil importDateUtil;
    private final String propertyKey;
    private final ThreadLocal<Calendar> calendarThreadLocal = new ThreadLocal<Calendar>()
    {
        @Override
        protected Calendar initialValue()
        {
            return LdbcDateCodec.newCalendar();
        }
    };

    public DateTimeDecorator( String propertyKey, ImportDateUtil importDateUtil )
    {
        this.propertyKey = propertyKey;
        this.importDateUtil = importDateUtil;
    }

    @Override
    public boolean isMutable()
    {
        return true;
    }

    @Override
    public InputEntity apply( InputEntity inputEntity ) throws RuntimeException
    {
        Calendar calendar = calendarThreadLocal.get();
        Object[] newProperties = inputEntity.properties();
        boolean propertyFound = false;
        for ( int i = 0; i < newProperties.length; i++ )
        {
            if ( newProperties[i].equals( propertyKey ) )
            {
                String dateTimeString = (String) newProperties[i + 1];
                try
                {
                    newProperties[i + 1] = importDateUtil.csvDateTimeToFormat( dateTimeString, calendar );
                }
                catch ( ParseException e )
                {
                    throw new RuntimeException( "Error while parsing date string: " + dateTimeString, e );
                }
                propertyFound = true;
                break;
            }
        }
        if ( !propertyFound )
        {
            throw new RuntimeException( "Could not find property: " + propertyKey );
        }

        Long newFirstPropertyId;
        try
        {
            newFirstPropertyId = inputEntity.firstPropertyId();
        }
        catch ( NullPointerException e )
        {
            newFirstPropertyId = null;
        }

        if ( inputEntity.getClass().equals( InputNode.class ) )
        {
            return new InputNode(
                    inputEntity.sourceDescription(),
                    inputEntity.lineNumber(),
                    inputEntity.position(),
                    ((InputNode) inputEntity).group(),
                    ((InputNode) inputEntity).id(),
                    newProperties,
                    newFirstPropertyId,
                    ((InputNode) inputEntity).labels(),
                    ((InputNode) inputEntity).labelField()
            );
        }
        else if ( inputEntity.getClass().equals( InputRelationship.class ) )
        {
            Integer newTypeId;
            try
            {
                newTypeId = ((InputRelationship) inputEntity).typeId();
            }
            catch ( NullPointerException e )
            {
                newTypeId = null;
            }

            return new InputRelationship(
                    inputEntity.sourceDescription(),
                    inputEntity.lineNumber(),
                    inputEntity.position(),
                    newProperties,
                    newFirstPropertyId,
                    ((InputRelationship) inputEntity).startNodeGroup(),
                    ((InputRelationship) inputEntity).startNode(),
                    ((InputRelationship) inputEntity).endNodeGroup(),
                    ((InputRelationship) inputEntity).endNode(),
                    ((InputRelationship) inputEntity).type(),
                    newTypeId
            );
        }
        else
        {
            throw new RuntimeException( "Unrecognized InputEntity subclass: " + inputEntity.getClass().getName() );
        }
    }
}
