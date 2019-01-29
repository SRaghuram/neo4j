package com.neo4j.bench.client.model;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.neo4j.bench.client.ClientUtil.generateUniqueId;

public class Annotation
{
    public static final String COMMENT = "comment";
    public static final String DATE = "date";
    public static final String EVENT_ID = "event_id";
    public static final String AUTHOR = "author";

    private final String comment;
    private final long date;
    private final String eventId;
    private final String author;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public Annotation()
    {
        this( "-1", -1, "-1", "-1" );
    }

    public Annotation( String comment, long date, String author )
    {
        this( comment, date, generateUniqueId(), author );
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
        Map<String,Object> map = new HashMap<>();
        map.put( COMMENT, comment );
        map.put( DATE, date );
        map.put( EVENT_ID, eventId );
        map.put( AUTHOR, author );
        return map;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        Annotation that = (Annotation) o;
        return date == that.date && Objects.equals( comment, that.comment ) &&
               Objects.equals( eventId, that.eventId ) && Objects.equals( author, that.author );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( comment, date, eventId, author );
    }

    @Override
    public String toString()
    {
        SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" );
        return "(" + comment + "," + format.format( new Date( date ) ) + "," + eventId + "," + author + "";
    }
}
