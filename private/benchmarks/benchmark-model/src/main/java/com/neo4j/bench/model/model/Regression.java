/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Regression
{
    public enum Status
    {
        REPORTED,
        FIXED,
        REJECTED
    }

    private final String comment;
    private final long date;
    private final String eventId;
    private final String author;
    private final Status status;
    private final URL trelloCardUrl;
    private final String baselineBranch;

    private Regression( String comment,
                       long date,
                       String eventId,
                       String author,
                       Status status,
                       URL trelloCardUrl,
                       String baselineBranch )
    {
        this.comment = comment;
        this.date = date;
        this.eventId = eventId;
        this.author = author;
        this.status = status;
        this.trelloCardUrl = trelloCardUrl;
        this.baselineBranch = baselineBranch;
    }

    public static Regression create( String comment,
                       String author,
                       Status status,
                       URL trelloCardUrl,
                       String baselineBranch )
    {
        return new Regression( comment,
              new Date().getTime(),
              UUID.randomUUID().toString(),
              author,
              status,
              trelloCardUrl,
              baselineBranch );
    }

    public Map<String,Object> toParams()
    {
        return new HashMap<String,Object>()
        {{
            put( "comment", comment );
            put( "date", date );
            put( "event_id", eventId );
            put( "author", author );
            put( "status", status.name() );
            put( "trello_card_url", trelloCardUrl.toString() );
            put( "baseline_branch", baselineBranch );
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
        return "Regression { " + Joiner.on( ", " ).withKeyValueSeparator( "=" ).join( toParams() ) + " }";
    }
}
