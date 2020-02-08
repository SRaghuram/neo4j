package org.neo4j.storageengine;

import org.neo4j.counts.CountsVisitor;
import org.neo4j.internal.recordstorage.RecordState;
import org.neo4j.storageengine.api.CountsDelta;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageCommonCommand;

import java.util.Collection;

public class MyCountsRecordState extends CountsDelta implements RecordState
{
    @Override
    public void extractCommands( Collection<StorageCommand> target )
    {
        accept( new CommandCollector( target ), null );
    }

    // CountsDelta already implements hasChanges

    private static class CommandCollector extends CountsVisitor.Adapter
    {
        private final Collection<StorageCommand> commands;

        CommandCollector( Collection<StorageCommand> commands )
        {
            this.commands = commands;
        }

        @Override
        public void visitNodeCount( int labelId, long count )
        {
            if ( count != 0 )
            {   // Only add commands for counts that actually change
                commands.add( new StorageCommonCommand.NodeCountsCommand( labelId, count ) );
            }
        }

        @Override
        public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
        {
            if ( count != 0 )
            {   // Only add commands for counts that actually change
                commands.add( new StorageCommonCommand.RelationshipCountsCommand( startLabelId, typeId, endLabelId, count ) );
            }
        }
    }
}
