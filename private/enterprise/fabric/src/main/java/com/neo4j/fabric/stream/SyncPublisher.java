/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class SyncPublisher implements Publisher<Record>
{

    private final Rx2SyncStream input;

    public SyncPublisher( Rx2SyncStream input )
    {
        this.input = input;
    }

    @Override
    public void subscribe( Subscriber<? super Record> subscriber )
    {
        SyncSubscription subscription = new SyncSubscription( input, subscriber );
        subscriber.onSubscribe( subscription );
    }

    static class SyncSubscription implements Subscription
    {
        private final Rx2SyncStream input;
        private final Subscriber<? super Record> output;
        private boolean running = true;

        SyncSubscription( Rx2SyncStream input, Subscriber<? super Record> output )
        {
            this.input = input;
            this.output = output;
        }

        @Override
        public void request( long n )
        {
            try
            {
                for ( int i = 0; i < n && running; i++ )
                {
                    Record record = input.readRecord();
                    if ( record == null )
                    {
                        output.onComplete();
                        running = false;
                    }
                    else
                    {
                        output.onNext( record );
                    }
                }
            }
            catch ( Throwable t )
            {
                output.onError( t );
            }
        }

        @Override
        public void cancel()
        {
            running = false;
            input.close();
        }
    }
}
