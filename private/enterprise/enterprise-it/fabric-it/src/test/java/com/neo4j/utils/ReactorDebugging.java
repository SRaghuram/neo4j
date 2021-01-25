/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.utils;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.fabric.stream.Record;

public class ReactorDebugging
{

    public static Publisher<Object> toStringInterceptor( Publisher<Object> pub )
    {
        if ( pub instanceof Mono )
        {
            return new Mono<>()
            {
                @Override
                public void subscribe( CoreSubscriber<? super Object> sub )
                {
                    pub.subscribe( toStringSubscriber( pub, sub ) );
                }
            };
        }
        else if ( pub instanceof Flux )
        {
            return new Flux<>()
            {
                @Override
                public void subscribe( CoreSubscriber<? super Object> sub )
                {
                    pub.subscribe( toStringSubscriber( pub, sub ) );
                }
            };
        }
        else
        {
            throw new RuntimeException( "unknown pub type " + pub.getClass().getSimpleName() );
        }
    }

    public static Subscriber<Object> toStringSubscriber( final Publisher<Object> pub, Subscriber<Object> subscriber )
    {
        return new Subscriber<>()
        {
            private void printEvent( String event, Object data )
            {
                if ( data == null )
                {
                    var str = String.format( "%s, %d -> %s", pub.getClass().getSimpleName(), pub.hashCode(), event );
                    System.out.println( str );
                }
                else if ( data instanceof Record )
                {
                    var record = (Record) data;
                    String rec = IntStream.range( 0, record.size() )
                                          .mapToObj( i -> record.getValue( i ).toString() )
                                          .collect( Collectors.joining( ", ", "[", "]" ) );
                    var str = String.format( "%s, %d -> %s: %s", pub.getClass().getSimpleName(), pub.hashCode(), event, rec );
                    System.out.println( str );
                }
                else
                {
                    var str = String.format( "%s, %d -> %s: %s", pub.getClass().getSimpleName(), pub.hashCode(), event, data );
                    System.out.println( str );
                }
            }

            @Override
            public void onSubscribe( Subscription subscription )
            {
                printEvent( "onSubscribe", null );
                subscriber.onSubscribe( subscription );
            }

            @Override
            public void onNext( Object o )
            {
                printEvent( "onNext", o );
                subscriber.onNext( o );
            }

            @Override
            public void onError( Throwable t )
            {
                printEvent( "onError", t );
                subscriber.onError( t );
            }

            @Override
            public void onComplete()
            {
                printEvent( "onComplete", null );
                subscriber.onComplete();
            }
        };
    }
}
