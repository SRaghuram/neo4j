/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.connection.ImportDateUtil;
import com.neo4j.bench.ldbc.connection.LdbcDateCodecUtil;

import java.text.ParseException;
import java.util.Calendar;
import java.util.function.Supplier;

import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.internal.batchimport.input.csv.Decorator;

public class PersonDecorator implements Decorator
{
    private final Supplier<ImportDateUtil> importDateUtilSupplier;

    public PersonDecorator( Supplier<ImportDateUtil> importDateUtilSupplier )
    {
        this.importDateUtilSupplier = importDateUtilSupplier;
    }

    @Override
    public InputEntityVisitor apply( InputEntityVisitor inputEntityVisitor )
    {
        // persons: id|firstName|lastName|gender|birthday|creationDate|locationIP|browserUsed|
        //         [id,_,firstName,_,lastName,_,gender,_,birthday,_,creationDate, _,locationIP,  _,browserUsed, _]
        //         [ 0,1,        2,3,       4,5,     6,7,       8,9,          10,11,        12, 13,         14,15]
        return new InputEntityVisitor.Delegate( inputEntityVisitor )
        {
            private final Calendar calendar = LdbcDateCodecUtil.newCalendar();
            private final ImportDateUtil importDateUtil = importDateUtilSupplier.get();

            @Override
            public boolean property( String key, Object value )
            {
                if ( "birthday".equals( key ) )
                {
                    String birthdayString = (String) value;
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
                    return super.property( Person.BIRTHDAY, birthday ) &&
                           super.property( Person.BIRTHDAY_MONTH, birthdayMonth ) &&
                           super.property( Person.BIRTHDAY_DAY_OF_MONTH, birthdayDayOfMonth );
                }
                else if ( "creationDate".equals( key ) )
                {
                    String creationDateString = (String) value;
                    try
                    {
                        return super.property(
                                key,
                                importDateUtil.csvDateTimeToFormat( creationDateString, calendar ) );
                    }
                    catch ( ParseException e )
                    {
                        throw new RuntimeException( "Error while parsing date string: " + creationDateString, e );
                    }
                }
                else
                {
                    return super.property( key, value );
                }
            }
        };
    }
}
