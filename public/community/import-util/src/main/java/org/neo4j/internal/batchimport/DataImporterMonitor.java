package org.neo4j.internal.batchimport;

import java.util.concurrent.atomic.LongAdder;

import static java.lang.String.format;

public class DataImporterMonitor {
        private final LongAdder nodes = new LongAdder();
        private final LongAdder relationships = new LongAdder();
        private final LongAdder properties = new LongAdder();

        public void nodesImported( long nodes )
        {
            this.nodes.add( nodes );
        }

        public void nodesRemoved( long nodes )
        {
            this.nodes.add( -nodes );
        }

        public void relationshipsImported( long relationships )
        {
            this.relationships.add( relationships );
        }

        public void propertiesImported( long properties )
        {
            this.properties.add( properties );
        }

        public void propertiesRemoved( long properties )
        {
            this.properties.add( -properties );
        }

        public long nodesImported()
        {
            return this.nodes.sum();
        }

        public long propertiesImported()
        {
            return this.properties.sum();
        }

        @Override
        public String toString()
        {
            return format( "Imported:%n  %d nodes%n  %d relationships%n  %d properties",
                    nodes.sum(), relationships.sum(), properties.sum() );
        }
}
