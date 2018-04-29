package edu.uchicago.cs.encsel.query;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.PrimitiveType;

public class MyConverter extends PrimitiveConverter {

    private Dictionary dictionary;

    private PrimitiveType type;

    private Object value;

    private PrimitiveConverter next = NonePrimitiveConverter.INSTANCE;

    public MyConverter(PrimitiveType type) {
        this.type = type;
    }

    public void setNext(PrimitiveConverter next) {
        if(next == null)
            throw new IllegalArgumentException("Next is null");
        this.next = next;
    }

    @Override
    public boolean hasDictionarySupport() {
        return true;
    }


    @Override
    public void setDictionary(Dictionary dictionary) {
        this.dictionary = dictionary;
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
        switch (this.type.getPrimitiveTypeName()) {
            case BINARY:
                addBinary(this.dictionary.decodeToBinary(dictionaryId));
                break;
            case FLOAT:
                addFloat(this.dictionary.decodeToFloat(dictionaryId));
                break;
            case DOUBLE:
                addDouble(this.dictionary.decodeToDouble(dictionaryId));
                break;
            case INT32:
                //addInt(this.dictionary.decodeToInt(dictionaryId));
                addInt(dictionaryId);
                break;
            case INT64:
                addLong(this.dictionary.decodeToLong(dictionaryId));
                break;
            case BOOLEAN:
                addBoolean(this.dictionary.decodeToBoolean(dictionaryId));
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public void addBinary(Binary value) {
        this.value = value;
        this.next.addBinary(value);
    }

    @Override
    public void addBoolean(boolean value) {
        this.value = value;
        this.next.addBoolean(value);
    }

    @Override
    public void addDouble(double value) {
        this.value = value;
        this.next.addDouble(value);
    }

    @Override
    public void addFloat(float value) {
        this.value = value;
        this.next.addFloat(value);
    }

    @Override
    public void addInt(int value) {
        this.value = value;
        this.next.addInt(value);
    }

    @Override
    public void addLong(long value) {
        this.value = value;
        this.next.addLong(value);
    }


}
