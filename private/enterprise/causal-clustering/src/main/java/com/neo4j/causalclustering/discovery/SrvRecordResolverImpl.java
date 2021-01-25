/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Optional;
import java.util.Properties;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.InitialDirContext;

public class SrvRecordResolverImpl extends SrvRecordResolver
{
    private final String[] SRV_RECORDS = {"SRV"};
    private final String SRV_ATTR = "srv";

    private Optional<InitialDirContext> _idc = Optional.empty();

    @Override
    public Stream<SrvRecord> resolveSrvRecord( String url ) throws NamingException
    {
        Attributes attrs = _idc.orElseGet( this::setIdc ).getAttributes( url, SRV_RECORDS );

        return enumerationAsStream( (NamingEnumeration<String>) attrs.get( SRV_ATTR ).getAll() ).map( SrvRecord::parse );
    }

    private synchronized InitialDirContext setIdc()
    {
        return _idc.orElseGet( () ->
        {
            Properties env = new Properties();
            env.put( Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory" );
            try
            {
                _idc = Optional.of( new InitialDirContext( env ) );
                return _idc.get();
            }
            catch ( NamingException e )
            {
                throw new RuntimeException( e );
            }
        } );
    }

    private static <T> Stream<T> enumerationAsStream( Enumeration<T> e )
    {
        return StreamSupport.stream( Spliterators.spliteratorUnknownSize( new Iterator<>()
        {
            @Override
            public T next()
            {
                return e.nextElement();
            }

            @Override
            public boolean hasNext()
            {
                return e.hasMoreElements();
            }
        }, Spliterator.ORDERED ), false );
    }
}
