/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Map;
import java.util.function.Function;

import static java.lang.String.format;

class StatusDescriptionFieldMatcher<T> extends TypeSafeMatcher<Map<String,Object>>
{
    private final Function<Object,T> converter;
    private final String fieldName;
    private final Matcher<T> matcher;

    StatusDescriptionFieldMatcher( Function<Object,T> converter, String fieldName, Matcher<T> matcher )
    {
        this.converter = converter;
        this.fieldName = fieldName;
        this.matcher = matcher;
    }

    @Override
    protected boolean matchesSafely( Map<String,Object> item )
    {
        Object field = item.get( fieldName );
        if ( field == null )
        {
            return false;
        }
        T value = converter.apply( field );
        return matcher.matches( value );
    }

    @Override
    public void describeTo( Description description )
    {
        description.appendText( format( "Field `%s` does not match ", fieldName ) );
        matcher.describeTo( description );
    }
}
