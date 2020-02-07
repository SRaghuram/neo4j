package org.neo4j.storageengine.api;

import org.neo4j.internal.recordstorage.CommonCommandVisitor;
import org.neo4j.internal.recordstorage.NeoCommandType;
import org.neo4j.io.fs.WritableChannel;

import java.io.IOException;

import static org.neo4j.token.api.TokenIdPrettyPrinter.label;
import static org.neo4j.token.api.TokenIdPrettyPrinter.relationshipType;

public abstract class StorageCommonCommand implements StorageCommand {
    protected int keyHash;
    protected long key;
    protected Mode mode;
    /*
     * TODO: This is techdebt
     * This is used to control the order of how commands are applied, which is done because
     * we don't take read locks, and so the order or how we change things lowers the risk
     * of reading invalid state. This should be removed once eg. MVCC or read locks has been
     * implemented.
     */
    public enum Mode
    {
        CREATE,
        UPDATE,
        DELETE;

        public static Mode fromRecordState( boolean created, boolean inUse )
        {
            if ( !inUse )
            {
                return DELETE;
            }
            if ( created )
            {
                return CREATE;
            }
            return UPDATE;
        }
    }

    public abstract boolean handle( CommonCommandVisitor handler ) throws IOException;

    protected final void setup( long key, Mode mode )
    {
        this.mode = mode;
        this.keyHash = (int) ((key >>> 32) ^ key);
        this.key = key;
    }

    public static class NodeCountsCommand extends StorageCommonCommand
    {
        private final int labelId;
        private final long delta;

        public NodeCountsCommand( int labelId, long delta )
        {
            setup( labelId, Mode.UPDATE );
            assert delta != 0 : "Tried to create a NodeCountsCommand for something that didn't change any count";
            this.labelId = labelId;
            this.delta = delta;
        }

        @Override
        public String toString()
        {
            return String.format( "UpdateCounts[(%s) %s %d]",
                    label( labelId ), delta < 0 ? "-" : "+", Math.abs( delta ) );
        }

        @Override
        public boolean handle( CommonCommandVisitor handler ) throws IOException
        {
            return handler.visitNodeCountsCommand( this );
        }

        public int labelId()
        {
            return labelId;
        }

        public long delta()
        {
            return delta;
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.put( NeoCommandType.UPDATE_NODE_COUNTS_COMMAND );
            channel.putInt( labelId() )
                    .putLong( delta() );
        }
    }

    public static class RelationshipCountsCommand extends StorageCommonCommand
    {
        private final int startLabelId;
        private final int typeId;
        private final int endLabelId;
        private final long delta;

        public RelationshipCountsCommand( int startLabelId, int typeId, int endLabelId, long delta )
        {
            setup( typeId, Mode.UPDATE );
            assert delta !=
                    0 : "Tried to create a RelationshipCountsCommand for something that didn't change any count";
            this.startLabelId = startLabelId;
            this.typeId = typeId;
            this.endLabelId = endLabelId;
            this.delta = delta;
        }

        @Override
        public String toString()
        {
            return String.format( "UpdateCounts[(%s)-%s->(%s) %s %d]",
                    label( startLabelId ), relationshipType( typeId ), label( endLabelId ),
                    delta < 0 ? "-" : "+", Math.abs( delta ) );
        }

        @Override
        public boolean handle( CommonCommandVisitor handler ) throws IOException
        {
            return handler.visitRelationshipCountsCommand( this );
        }

        public int startLabelId()
        {
            return startLabelId;
        }

        public int typeId()
        {
            return typeId;
        }

        public int endLabelId()
        {
            return endLabelId;
        }

        public long delta()
        {
            return delta;
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.put( NeoCommandType.UPDATE_RELATIONSHIP_COUNTS_COMMAND );
            channel.putInt( startLabelId() )
                    .putInt( typeId() )
                    .putInt( endLabelId() )
                    .putLong( delta() );
        }
    }

}

