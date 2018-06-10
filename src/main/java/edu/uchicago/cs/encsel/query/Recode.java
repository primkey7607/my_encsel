package edu.uchicago.cs.encsel.query;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.uchicago.cs.encsel.model.IntEncoding;
import edu.uchicago.cs.encsel.parquet.EncContext;
import edu.uchicago.cs.encsel.parquet.EncReaderProcessor;
import edu.uchicago.cs.encsel.parquet.ParquetReaderHelper;
import edu.uchicago.cs.encsel.parquet.ParquetWriterHelper;
import edu.uchicago.cs.encsel.query.bitmap.RoaringBitmap;
import edu.uchicago.cs.encsel.query.util.DataUtils;
import edu.uchicago.cs.encsel.query.util.SchemaUtils;
import org.apache.parquet.it.unimi.dsi.fastutil.objects.Object2IntMap;
import edu.uchicago.cs.encsel.query.tpch.TPCHSchema;

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

    public ArrayList<ColumnReaderImpl2> map2(VersionParser.ParsedVersion version,
                                           PageReadStore rowGroup,
                                           List<ColumnDescriptor> arr){
        ArrayList<ColumnReaderImpl2> result = new ArrayList<>();
        for (ColumnDescriptor a : arr){
            ColumnReaderImpl2 cri = new ColumnReaderImpl2(a, rowGroup.getPageReader(a),
                    hashRecorder.getConverter(a.getPath()).asPrimitiveConverter(), version);
            result.add(cri);
        }
        return result;
    }

    public void addValues(){
        MessageType projected = SchemaUtils.project(schema, col);
        this.hashRecorder = new RowTempTable(projected);

        // Build Hash Table
        try{
            ParquetReaderHelper.read(new File(filepath + ".parquet").toURI(), new EncReaderProcessor() {

                public void processRowGroup(VersionParser.ParsedVersion version, BlockMetaData meta, PageReadStore rowGroup) {
                    ArrayList<ColumnReaderImpl> hashRowReaders = map(version, rowGroup, projected.getColumns());

                    ColumnReaderImpl hashKeyReader = hashRowReaders.get(0); //there should only be one element here
                    for (int i = 0; i < rowGroup.getRowCount(); i++) {
                        Integer hashKey = -1;
                        Object o = DataUtils.readValue(hashKeyReader);
                        if (o instanceof Integer){
                            hashKey = (Integer) o;
                        }else{
                            throw new IllegalArgumentException("Expected Integer, received other type");
                        }
                        //System.out.printf("hashKey: %d\n", hashKey ); seems to work
                        values.add(hashKey);
                        hashKeyReader.consume();

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

        }catch (IOException e){
            e.printStackTrace();
        }catch (VersionParser.VersionParseException e){
            e.printStackTrace();
        }
    }

    public void addCodes(){
        MessageType projected = SchemaUtils.project(schema, col);
        this.hashRecorder = new RowTempTable(projected);

        // Build Hash Table
        try{
            ParquetReaderHelper.read(new File(filepath + ".parquet").toURI(), new EncReaderProcessor() {

                public void processRowGroup(VersionParser.ParsedVersion version, BlockMetaData meta, PageReadStore rowGroup) {
                    ArrayList<ColumnReaderImpl2> hashRowReaders = map2(version, rowGroup, projected.getColumns());

                    ColumnReaderImpl2 hashKeyReader = hashRowReaders.get(0); //there should only be one element here
                    for (int i = 0; i < rowGroup.getRowCount(); i++) {
                        Integer hashKey = hashKeyReader.getCurrentValueDictionaryID();
                        //System.out.printf("hashKey: %d\n", hashKey );
                        codes.add(hashKey);
                        hashKeyReader.consume();

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

        }catch (IOException e){
            e.printStackTrace();
        }catch (VersionParser.VersionParseException e){
            e.printStackTrace();
        }
    }

    public HashMap<Integer,Integer> createMap() throws IllegalArgumentException{
        /* Design: Read values using ColumnReader,
         Read codes using ColumnReader2
         Zip the two together and return the resulting hashmap
         */
        HashMap<Integer,Integer> result = new HashMap<>();
        this.addValues();
        this.addCodes();
        if (this.codes.size() != this.values.size()){
            throw new IllegalArgumentException("ERROR: values do not correspond to codes");
        }
        for (int i = 0; i < this.codes.size(); i++){
            result.putIfAbsent(this.values.get(i), this.codes.get(i));
        }


        return result;

    }

    public void makeGlobalDict(){
        //I suppose this was made specifically for the case where lineitem is given, and part is used here.
        //I will make it general later
        String part = "../tpch-generator/dbgen/part";
        String lineitem = "../tpch-generator/dbgen/lineitem";
        URI[] input = {new File(this.filepath+".tbl").toURI()};
        int[] index = this.col;
        MessageType[] schemas = {schema};
        Boolean order = true;
        int intbound = ParquetWriterHelper.scanIntMaxInTab(new File(lineitem+".tbl").toURI(), 1);
        int pib = ParquetWriterHelper.scanIntMaxInTab(new File(part+".tbl").toURI(), 0);
        int pbl = 32 - Integer.numberOfLeadingZeros(intbound);
        Object2IntMap dictMap = ParquetWriterHelper.buildGlobalDict(input,index,schemas,order);
        EncContext.encoding.get().put(TPCHSchema.partSchema().getColumns().get(0).toString(), IntEncoding.valueOf("DICT").parquetEncoding());
        EncContext.context.get().put(TPCHSchema.partSchema().getColumns().get(0).toString(), new Integer[]{pbl,pib});
        EncContext.globalDict.get().put(TPCHSchema.partSchema().getColumns().get(0).toString(), dictMap);

        ParquetWriterHelper.write(new File(part+".tbl").toURI(), TPCHSchema.partSchema(),
                new File(part+".parquet").toURI(), "\\|", false, "UNCOMPRESSED");
    }
}
