/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License,
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 *
 */

package edu.uchicago.cs.encsel.dataset.feature;

import edu.uchicago.cs.encsel.dataset.persist.jpa.ColumnWrapper;
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence;
import edu.uchicago.cs.encsel.model.FloatEncoding;
import edu.uchicago.cs.encsel.model.IntEncoding;
import edu.uchicago.cs.encsel.model.LongEncoding;
import edu.uchicago.cs.encsel.model.StringEncoding;
import edu.uchicago.cs.encsel.parquet.ParquetWriterHelper;

import javax.persistence.EntityManager;
import java.util.List;


public class DictEncodeAllCols {

    public static void main(String[] args) throws Exception {
        final int start = (args.length >= 1) ? Integer.parseInt(args[0]) : 0;
        int batch = 2000;
        System.out.println(String.format("Start is %d", start));
        EntityManager em = JPAPersistence.emf().createEntityManager();
        List<ColumnWrapper> columns = em.createQuery("select c from Column c where c.parentWrapper IS NULL", ColumnWrapper.class).getResultList();
        System.out.println("columns in total:" + columns.size());
        for (ColumnWrapper col : columns){
            System.out.println(col.id());
            if (col.id() >= start) {
                System.out.println(col.colFile().toString());
                switch (col.dataType()) {
                    case STRING:
                        DictionaryEncoder.singleColumnString(col.colFile(), batch);
                        DictionaryEncoder.singleColumnString(col.colFile(), Integer.MAX_VALUE);
                        break;
                    case LONG:
                        DictionaryEncoder.singleColumnLong(col.colFile(), batch);
                        DictionaryEncoder.singleColumnLong(col.colFile(), Integer.MAX_VALUE);
                        break;
                    case INTEGER:
                        DictionaryEncoder.singleColumnInt(col.colFile(), batch);
                        DictionaryEncoder.singleColumnInt(col.colFile(), Integer.MAX_VALUE);
                        break;
                    case FLOAT:
                        DictionaryEncoder.singleColumnFloat(col.colFile(), batch);
                        DictionaryEncoder.singleColumnFloat(col.colFile(), Integer.MAX_VALUE);
                        break;
                    case DOUBLE:
                        DictionaryEncoder.singleColumnDouble(col.colFile(), batch);
                        DictionaryEncoder.singleColumnDouble(col.colFile(), Integer.MAX_VALUE);
                        break;
                    case BOOLEAN:
                        break;
                }
                System.out.println("Finished");
            }
        }
        System.out.println("Ended successfully");
    }
}
