/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class Annotation
{
    private final String comment;
    private final long date;
    private final String eventId;
    private final String author;

    public static Annotation create( String comment, String author )
    {
        return new Annotation( comment, System.currentTimeMillis(), UUID.randomUUID().toString(), author );
    }

    /**
     * WARNING: Never call this explicitly. No-params constructor is only used for JSON (de)serialization.
     */
    public Annotation()
    {
        this( "-1", -1, "-1", "-1" );
    }

    public Annotation( String comment, long date, String author )
    {
        this( comment, date, UUID.randomUUID().toString(), author );
    }

    public Annotation( String comment, long date, String eventId, String author )
    {
        this.comment = comment;
        this.date = date;
        this.eventId = eventId;
        this.author = author;
    }

    public String eventId()
    {
        return eventId;
    }

    public Map<String,Object> toMap()
    {
        return new HashMap<String,Object>()
        {{
            put( "comment", comment );
            put( "date", date );
            put( "event_id", eventId );
            put( "author", author );
        }};
    }

    @Override
    public boolean equals( Object o )
    {
        return EqualsBuilder.reflectionEquals( this, o );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    @Override
    public String toString()
    {
        SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" );
        return "(" + comment + "," + format.format( new Date( date ) ) + "," + eventId + "," + author + "";
    }
}
