package edu.uchicago.cs.encsel.query;

import edu.uchicago.cs.encsel.parquet.EncContext;
import edu.uchicago.cs.encsel.parquet.ParquetWriterHelper;
import edu.uchicago.cs.encsel.query.Recode;
import edu.uchicago.cs.encsel.query.tpch.TPCHSchema;

import java.io.File;
import java.util.HashMap;

public class RecodeTool {

    public static void main(String[] args) {
        args = new String[]{"DICT","DICT", "UNCOMPRESSED"};
        if (args.length == 0) {
            System.out.println("JoinFileProducer PPencoding LPencoding Compression");
            return;
        }
        String PPencoding = args[0];
        String LPencoding = args[1];
        String comp = args[2];
        String lineitem = "../tpch-generator/dbgen/lineitem";
        String part = "../tpch-generator/dbgen/part";
        int intbound = ParquetWriterHelper.scanIntMaxInTab(new File(lineitem+".tbl").toURI(), 1);
        int bitLength = 32 - Integer.numberOfLeadingZeros(intbound);
        System.out.println("lineitem intBitLength: "+ bitLength +" lineitem intBound: "+intbound);

        int pib = ParquetWriterHelper.scanIntMaxInTab(new File(part+".tbl").toURI(), 0);
        int pbl = 32 - Integer.numberOfLeadingZeros(intbound);
        System.out.println("part intBitLength: "+ bitLength +" part intBound: "+intbound);

        EncContext.context.get().put(TPCHSchema.lineitemSchema().getColumns().get(1).toString(), new Object[]{bitLength,intbound});
        EncContext.context.get().put(TPCHSchema.partSchema().getColumns().get(0).toString(), new Object[]{pbl,pib});
        //Just perform "recoding" once, and see what it prints
        Recode r = new Recode(lineitem, TPCHSchema.lineitemSchema(), new int[]{1});
        //HashMap<Integer,Integer> h = r.createMap();
        //System.out.printf("Size of HashMap: %d\n", h.size());
        r.makeGlobalDict();

    }
}
