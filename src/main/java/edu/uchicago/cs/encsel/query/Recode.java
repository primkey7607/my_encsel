package edu.uchicago.cs.encsel.query;

import java.net.URI
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.uchicago.cs.encsel.parquet.EncReaderProcessor;
import edu.uchicago.cs.encsel.parquet.ParquetReaderHelper;
import edu.uchicago.cs.encsel.query.bitmap.RoaringBitmap;
import edu.uchicago.cs.encsel.query.util.DataUtils;
import edu.uchicago.cs.encsel.query.util.SchemaUtils;

import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.impl.ColumnReaderImpl2;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;

public class Recode {

    private String filepath;
    private MessageType schema;
    private int[] col;
    private ArrayList<Integer> values;
    private ArrayList<Integer> codes;
    private RowTempTable hashRecorder;

    public Recode(String filepath, MessageType schema, int[] col){
        this.filepath = filepath;
        this.schema = schema;
        this.col = col;
        this.values = new ArrayList<>();
        this.codes = new ArrayList<>();
    }

    public ArrayList<ColumnReaderImpl> map(VersionParser.ParsedVersion version,
                                           PageReadStore rowGroup,
                                           List<ColumnDescriptor> arr){
        ArrayList<ColumnReaderImpl> result = new ArrayList<>();
        for (ColumnDescriptor a : arr){
            ColumnReaderImpl cri = new ColumnReaderImpl(a, rowGroup.getPageReader(a),
                    hashRecorder.getConverter(a.getPath()).asPrimitiveConverter(), version);
            result.add(cri);
        }
        return result;
    }

    public HashMap<Integer,Integer> createMap(){
        /* Design: Read values using ColumnReader,
         Read codes using ColumnReader2
         Zip the two together and return the resulting hashmap
         */
        MessageType projected = SchemaUtils.project(schema, col);
        this.hashRecorder = new RowTempTable(projected);

        // Build Hash Table
        ParquetReaderHelper.read(filepath, new EncReaderProcessor() {

            public void processRowGroup(VersionParser.ParsedVersion version, BlockMetaData meta, PageReadStore rowGroup) {
                ArrayList<ColumnReaderImpl> hashRowReaders = map(version, rowGroup, projected.getColumns());

                ColumnReaderImpl hashKeyReader = hashRowReaders.get(0); //there should only be one element here
                for (int i = 0; i < rowGroup.getRowCount(); i++) {
                    int hashKey = DataUtils.readValue(hashKeyReader);

                    //println(hashKey)
                    //println(hashKeyReader.getCurrentValueDictionaryID)

                    //hashtable.put(hashKey, hashRecorder.getCurrentRecord)
                    //hashRecorder.start()
                    /*if (reader.equals(hashKeyReader)){
                        reader.writeCurrentValueToConverter()
                        reader.consume()
                        //skipped = true
                    }else{
                        reader.writeCurrentValueToConverter()
                        reader.consume()
                    }*/
                    //hashRecorder.end()

                }
            }
        });

        return new HashMap<>(); //WARNING: dummy return value

    }
}
