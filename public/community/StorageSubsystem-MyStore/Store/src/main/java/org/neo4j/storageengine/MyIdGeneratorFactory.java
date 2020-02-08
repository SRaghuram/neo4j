package org.neo4j.storageengine;

import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.internal.id.*;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public class MyIdGeneratorFactory implements IdGeneratorFactory {
    IdGenerator[] idGenerators = new IdGenerator[IdType.values().length];
    private final EnumMap<IdType, IdGenerator> generators = new EnumMap<>( IdType.class );
    @Override
    public IdGenerator open(PageCache pageCache, File filename, IdType idType, LongSupplier highIdScanner, long maxId, boolean readOnly,
                            PageCursorTracer cursorTracer, ImmutableSet<OpenOption> openOptions) {
        if (idGenerators[idType.ordinal()] == null)
            idGenerators[idType.ordinal()] = new myIdGenerator();
        return idGenerators[idType.ordinal()];
    }

    @Override
    public IdGenerator create(PageCache pageCache, File filename, IdType idType, long highId, boolean throwIfFileExists, long maxId, boolean readOnly,
                              PageCursorTracer cursorTracer, ImmutableSet<OpenOption> openOptions) {
        if (idGenerators[idType.ordinal()] == null)
            idGenerators[idType.ordinal()] = new myIdGenerator();
        return idGenerators[idType.ordinal()];
    }



    @Override
    public IdGenerator get(IdType idType) {
        return idGenerators[idType.ordinal()];
    }

    @Override
    public void visit(Consumer<IdGenerator> visitor) {

    }

    @Override
    public void clearCache( PageCursorTracer cursorTracer ) {

    }

    @Override
    public Collection<File> listIdFiles() {
        return Arrays.stream(new File[0]).collect( Collectors.toList() );
    }

    //---------------------------------------------------------
    public class myIdGenerator implements IdGenerator
    {
        long highId = 0;
        @Override
        public void setHighId(long id) {
            highId = id;
        }

        @Override
        public void markHighestWrittenAtHighId() {

        }

        @Override
        public long getHighId() {
            return highId;
        }

        @Override
        public long getHighestPossibleIdInUse() {
            return highId-1;
        }

        @Override
        public Marker marker( PageCursorTracer cursorTracer ) {
            return null;
        }


        @Override
        public void close() {

        }

        @Override
        public long getNumberOfIdsInUse() {
            return highId;
        }

        @Override
        public long getDefragCount() {
            return 0;
        }

        @Override
        public void checkpoint(IOLimiter ioLimiter, PageCursorTracer cursorTracer) {

        }

        @Override
        public void maintenance( PageCursorTracer cursorTracer ) {

        }

        @Override
        public void start(FreeIds freeIdsForRebuild, PageCursorTracer cursorTracer) throws IOException {

        }

        @Override
        public void clearCache( PageCursorTracer cursorTracer ) {

        }

        @Override
        public long nextId( PageCursorTracer cursorTracer ) {
            return 0;
        }

        @Override
        public IdRange nextIdBatch(int size, PageCursorTracer cursorTracer ) {
            return null;
        }

        @Override
        public boolean consistencyCheck(ReporterFactory reporterFactory) {
            return false;
        }
    }
}
