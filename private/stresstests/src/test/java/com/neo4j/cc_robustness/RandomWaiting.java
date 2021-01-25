/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.kernel.impl.util.Listener;

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;

class RandomWaiting implements Listener<String>
{
    private volatile float probability;

    void enable( float probability )
    {
        this.probability = probability;
    }

    @Override
    public void receive( String notification )
    {
        float probability = this.probability;
        if ( probability < 0 )
        {
            return;
        }

        Random random = ThreadLocalRandom.current();
        if ( random.nextFloat() < probability )
        {
            try
            {
                int seconds = /*at least*/10 + /*and then*/random.nextInt( 7 );
                System.out.println( "Delaying message '" + notification + "' " + seconds + " seconds, on thread \"" + Thread.currentThread().getName() + "\"" );
                sleep( SECONDS.toMillis( seconds ) );
            }
            catch ( InterruptedException e )
            {
                // It's OK, this is just a test anyway
                throw new RuntimeException( e );
            }
        }
    }
}
