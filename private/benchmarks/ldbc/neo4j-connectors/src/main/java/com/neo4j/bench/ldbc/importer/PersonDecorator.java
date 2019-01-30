/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.neo4j.bench.ldbc.Domain;
import com.neo4j.bench.ldbc.connection.ImportDateUtil;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;

import java.text.ParseException;
import java.util.Calendar;

import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.csv.Decorator;

public class PersonDecorator implements Decorator<InputNode>
{
    private final ThreadLocal<Calendar> calendarThreadLocal = new ThreadLocal<Calendar>()
    {
        @Override
        protected Calendar initialValue()
        {
            return LdbcDateCodec.newCalendar();
        }
    };
    private final ImportDateUtil importDateUtil;
    private static final String[] EMPTY_STRING_ARRAY = new String[]{};

    public PersonDecorator( ImportDateUtil importDateUtil )
    {
        this.importDateUtil = importDateUtil;
    }

    @Override
    public InputNode apply( InputNode inputNode ) throws RuntimeException
    {
        Calendar calendar = calendarThreadLocal.get();
        // persons: id|firstName|lastName|gender|birthday|creationDate|locationIP|browserUsed|
        //         [id,_,firstName,_,lastName,_,gender,_,birthday,_,creationDate, _,locationIP,  _,browserUsed, _]
        //         [ 0,1,        2,3,       4,5,     6,7,       8,9,          10,11,        12, 13,         14,15]
        String birthdayString = (String) inputNode.properties()[9];
        long birthday;
        int birthdayMonth;
        int birthdayDayOfMonth;
        try
        {
            birthday = importDateUtil.csvDateToFormat( birthdayString, calendar );
            birthdayMonth = importDateUtil.queryDateUtil().formatToMonth( birthday );
            birthdayDayOfMonth = importDateUtil.queryDateUtil().formatToDay( birthday );
        }
        catch ( ParseException e )
        {
            throw new RuntimeException( "Error while parsing date string: " + birthdayString, e );
        }
        inputNode.properties()[9] = birthday;

        String creationDateString = (String) inputNode.properties()[11];
        long creationDate;
        try
        {
            creationDate = importDateUtil.csvDateTimeToFormat( creationDateString, calendar );
        }
        catch ( ParseException e )
        {
            throw new RuntimeException( "Error while parsing date string: " + creationDateString, e );
        }

        final int originalPropertiesLength = inputNode.properties().length;
        Object[] newProperties = new Object[originalPropertiesLength + 4];
        for ( int i = 0; i < originalPropertiesLength; i++ )
        {
            newProperties[i] = inputNode.properties()[i];
        }
        newProperties[11] = creationDate;
        newProperties[originalPropertiesLength] = Domain.Person.BIRTHDAY_MONTH;
        newProperties[originalPropertiesLength + 1] = birthdayMonth;
        newProperties[originalPropertiesLength + 2] = Domain.Person.BIRTHDAY_DAY_OF_MONTH;
        newProperties[originalPropertiesLength + 3] = birthdayDayOfMonth;

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
                inputNode.labels(),
                inputNode.labelField()
        );
    }
}
