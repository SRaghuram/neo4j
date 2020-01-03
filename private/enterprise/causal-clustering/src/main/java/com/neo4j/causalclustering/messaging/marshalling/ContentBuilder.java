/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import java.util.function.Function;

/** Used to lazily build object of given type where the resulting object may contain objects of the same type.
 *  Executes the composed function when {@link #build()} is called.
 * @param <CONTENT> type of the object that will be built.
 */
public class ContentBuilder<CONTENT>
{
    private boolean isComplete;
    private Function<CONTENT,CONTENT> contentFunction;

    public static <C> ContentBuilder<C> emptyUnfinished()
    {
        return new ContentBuilder<>( content -> content, false );
    }

    public static <C> ContentBuilder<C> unfinished( Function<C,C> contentFunction )
    {
        return new ContentBuilder<>( contentFunction, false );
    }

    public static <C> ContentBuilder<C> finished( C content )
    {
        return new ContentBuilder<>( ignored -> content, true );
    }

    private ContentBuilder( Function<CONTENT,CONTENT> contentFunction, boolean isComplete )
    {
        this.contentFunction = contentFunction;
        this.isComplete = isComplete;
    }

    /**  Signals that the object is ready to be built
     * @return true if builder is complete and ready to be built.
     */
    public boolean isComplete()
    {
        return isComplete;
    }

    /** Composes this with the given builder and updates {@link #isComplete()} with the provided builder.
     * @param contentBuilder that will be combined with this builder
     * @return The combined builder
     * @throws IllegalStateException if the current builder is already complete
     */
    public ContentBuilder<CONTENT> combine( ContentBuilder<CONTENT> contentBuilder )
    {
        if ( isComplete )
        {
            throw new IllegalStateException( "This content builder has already completed and cannot be combined." );
        }
        contentFunction = contentFunction.compose( contentBuilder.contentFunction );
        isComplete = contentBuilder.isComplete;
        return this;
    }

    /** Builds the object given type. Can only be called if {@link #isComplete()} is true.
     * @return the complete object
     * @throws IllegalStateException if {@link #isComplete()} is false.
     */
    public CONTENT build()
    {
        if ( !isComplete )
        {
            throw new IllegalStateException( "Cannot build unfinished content" );
        }
        return contentFunction.apply( null );
    }
}
