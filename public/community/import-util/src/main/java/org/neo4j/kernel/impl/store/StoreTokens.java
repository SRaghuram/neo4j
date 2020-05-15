package org.neo4j.kernel.impl.store;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokensLoader;

import java.util.List;

public class StoreTokens {
    private StoreTokens()
    {
    }

    /**
     * Get a {@link TokensLoader} that loads tokens by reading the relevant token stores from the given {@link BatchingStoreBase}.
     * <p>
     * Note that this will ignore any tokens that cannot be read, for instance due to a store inconsistency, or if the store needs to be recovered.
     * If you would rather have an exception thrown, then you need to {@link TokenHolder#setInitialTokens(List) set the initial tokens} on each of the token
     * holders,
     *
     * @param basicNeoStore The {@link BatchingStoreBase} to read tokens from.
     */
    public static TokensLoader allReadableTokens( BatchingStoreBase basicNeoStore )
    {
        return new TokensLoader()
        {
            @Override
            public List<NamedToken> getPropertyKeyTokens(PageCursorTracer cursorTracer )
            {
                return basicNeoStore.getPropertyKeyTokensReadable( cursorTracer );
            }

            @Override
            public List<NamedToken> getLabelTokens( PageCursorTracer cursorTracer )
            {
                return basicNeoStore.getLabelTokensReadable( cursorTracer );
            }

            @Override
            public List<NamedToken> getRelationshipTypeTokens( PageCursorTracer cursorTracer )
            {
                return basicNeoStore.getRelationshipTypeTokensReadable( cursorTracer );
            }
        };
    }

    /**
     * Get a {@link TokensLoader} that loads tokens by reading the relevant token stores from the given {@link BatchingStoreBase}.
     * <p>
     * This loader will throw exceptions if it encounters a token that cannot be read due to an inconsistency.
     *
     * @param basicNeoStore The {@link BatchingStoreBase} to read tokens from.
     */
    public static TokensLoader allTokens( BatchingStoreBase basicNeoStore )
    {
        return new TokensLoader()
        {
            @Override
            public List<NamedToken> getPropertyKeyTokens(PageCursorTracer cursorTracer )
            {
                return basicNeoStore.getPropertyKeyTokens( cursorTracer );
            }

            @Override
            public List<NamedToken> getLabelTokens( PageCursorTracer cursorTracer )
            {
                return basicNeoStore.getLabelTokens( cursorTracer );
            }

            @Override
            public List<NamedToken> getRelationshipTypeTokens( PageCursorTracer cursorTracer )
            {
                return basicNeoStore.getRelationshipTypeTokens( cursorTracer );
            }
        };
    }

    /**
     * Create read-only token holders initialised with the tokens from the given {@link BatchingStoreBase}.
     * <p>
     * Note that this call will ignore tokens that cannot be loaded due to inconsistencies, rather than throwing an exception.
     * The reason for this is that the read-only token holders are primarily used by tools, such as the consistency checker.
     *
     * @param basicNeoStore The {@link BatchingStoreBase} from which to load the initial tokens.
     * @return TokenHolders that can be used for reading tokens, but cannot create new ones.
     */
    public static TokenHolders readOnlyTokenHolders(BatchingStoreBase basicNeoStore, PageCursorTracer cursorTracer )
    {
        TokenHolder propertyKeyTokens = createReadOnlyTokenHolder( TokenHolder.TYPE_PROPERTY_KEY );
        TokenHolder labelTokens = createReadOnlyTokenHolder( TokenHolder.TYPE_LABEL );
        TokenHolder relationshipTypeTokens = createReadOnlyTokenHolder( TokenHolder.TYPE_RELATIONSHIP_TYPE );
        TokenHolders tokenHolders = new TokenHolders( propertyKeyTokens, labelTokens, relationshipTypeTokens );
        tokenHolders.setInitialTokens( allReadableTokens( basicNeoStore ), cursorTracer );
        return tokenHolders;
    }

    /**
     * Create an empty read-only token holder of the given type.
     * @param tokenType one of {@link TokenHolder#TYPE_LABEL}, {@link TokenHolder#TYPE_RELATIONSHIP_TYPE}, or {@link TokenHolder#TYPE_PROPERTY_KEY}.
     * @return An empty read-only token holder.
     */
    public static TokenHolder createReadOnlyTokenHolder( String tokenType )
    {
        return new DelegatingTokenHolder( new ReadOnlyTokenCreator(), tokenType );
    }

    public static TokenHolders getTokenHolders(BatchingStoreBase basicNeoStore, PageCursorTracer cursorTracer )
    {
        TokenHolder propertyKeyTokens = getTokenHolder( TokenHolder.TYPE_PROPERTY_KEY );
        TokenHolder labelTokens = getTokenHolder( TokenHolder.TYPE_LABEL );
        TokenHolder relationshipTypeTokens = getTokenHolder( TokenHolder.TYPE_RELATIONSHIP_TYPE );
        TokenHolders tokenHolders = new TokenHolders( propertyKeyTokens, labelTokens, relationshipTypeTokens );
        tokenHolders.setInitialTokens( allTokens( basicNeoStore ), cursorTracer );
        return tokenHolders;
    }

    /**
     * Create an empty read-only token holder of the given type.
     * @param tokenType one of {@link TokenHolder#TYPE_LABEL}, {@link TokenHolder#TYPE_RELATIONSHIP_TYPE}, or {@link TokenHolder#TYPE_PROPERTY_KEY}.
     * @return An empty read-only token holder.
     */
    public static TokenHolder getTokenHolder( String tokenType )
    {
        return new DelegatingTokenHolder( new ReadOnlyTokenCreator(), tokenType );
    }
}
