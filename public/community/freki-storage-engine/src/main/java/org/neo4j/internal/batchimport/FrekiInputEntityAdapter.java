package org.neo4j.internal.batchimport;

import org.neo4j.internal.batchimport.input.Group;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;

import java.io.IOException;

public class FrekiInputEntityAdapter implements InputEntityVisitor {
    @Override
    public boolean propertyId(long nextProp) {
        return false;
    }

    @Override
    public boolean property(String key, Object value) {
        return false;
    }

    @Override
    public boolean property(int propertyKeyId, Object value) {
        return false;
    }

    @Override
    public boolean property(String key, Object value, Object stringValue) {
        return false;
    }

    @Override
    public boolean id(long id) {
        return false;
    }

    @Override
    public boolean id(Object id, Group group) {
        return false;
    }

    @Override
    public boolean labels(String[] labels) {
        return false;
    }

    @Override
    public boolean labelField(long labelField) {
        return false;
    }

    @Override
    public boolean startId(long id) {
        return false;
    }

    @Override
    public boolean startId(Object id, Group group) {
        return false;
    }

    @Override
    public boolean endId(long id) {
        return false;
    }

    @Override
    public boolean endId(Object id, Group group) {
        return false;
    }

    @Override
    public boolean type(int type) {
        return false;
    }

    @Override
    public boolean type(String type) {
        return false;
    }

    @Override
    public void endOfEntity() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
