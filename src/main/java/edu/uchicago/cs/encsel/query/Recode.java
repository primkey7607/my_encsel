package edu.uchicago.cs.encsel.query;

import java.net.URI
import java.util.ArrayList;
import java.util.HashMap;

import edu.uchicago.cs.encsel.parquet.EncReaderProcessor;
import edu.uchicago.cs.encsel.parquet.ParquetReaderHelper;
import edu.uchicago.cs.encsel.query._
import edu.uchicago.cs.encsel.query.bitmap.RoaringBitmap;
import edu.uchicago.cs.encsel.query.util.DataUtils;
import edu.uchicago.cs.encsel.query.util.SchemaUtils;

import org.apache.parquet.VersionParser
import org.apache.parquet.column.impl.ColumnReaderImpl
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.schema.MessageType

public class Recode {

    private String filepath;
    private MessageType schema;
    private int[] col;
    private ArrayList<Integer> values;
    private ArrayList<Integer> codes;

    public Recode(String filepath, MessageType schema, int[] col){
        this.filepath = filepath;
        this.schema = schema;
        this.col = col;
        this.values = new ArrayList<>();
        this.codes = new ArrayList<>();
    }

    public HashMap<Integer,Integer> createMap(){
        /* Design: Read values using ColumnReader,
         Read codes using ColumnReader2
         Zip the two together and return the resulting hashmap
         */
        
    }
}
