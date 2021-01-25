/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.neo4j.bench.ldbc.connection.ImportDateUtil;
import com.neo4j.bench.ldbc.connection.LdbcDateCodecUtil;

import java.util.Calendar;
import java.util.function.Supplier;

import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.internal.batchimport.input.csv.Decorator;

public class DateTimeDecorator implements Decorator
{
    private final Supplier<ImportDateUtil> importDateUtilSupplier;
    private final String propertyKey;

    public DateTimeDecorator( String propertyKey, Supplier<ImportDateUtil> importDateUtilSupplier )
    {
        this.propertyKey = propertyKey;
        this.importDateUtilSupplier = importDateUtilSupplier;
    }

    @Override
    public boolean isMutable()
    {
        return true;
    }

    @Override
    public InputEntityVisitor apply( InputEntityVisitor inputEntityVisitor )
    {
        return new InputEntityVisitor.Delegate( inputEntityVisitor )
        {
            private final Calendar calendar = LdbcDateCodecUtil.newCalendar();
            private final ImportDateUtil importDateUtil = importDateUtilSupplier.get();

            @Override
            public boolean property( String key, Object value )
            {
                if ( key.equals( propertyKey ) )
                {
                    try
                    {
                        return super.property(
                                key,
                                importDateUtil.csvDateTimeToFormat( (String) value, calendar ) );
                    }
                    catch ( Exception e )
                    {
                        throw new RuntimeException( "Error decorating date:\n" +
                                                    "Key = '" + key + "'\n" +
                                                    "Value = '" + value + "'", e );
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
