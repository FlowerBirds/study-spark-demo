package com.meritdata.spark;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.*;

import java.util.List;
import java.util.Properties;

public class TestOrcReader {

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
        Path testFilePath = new Path("hdfs://localhost:9000/user/hive/warehouse121/statics_custom_crowd");
        Properties p = new Properties();
        OrcSerde serde = new OrcSerde();
        p.setProperty("columns", "statics_pid,statics_user_id,statics_p_name,statics_in_time_start,statics_in_time_end,statics_in_formula,statics_in_formula_new,static_field,static_field_organ,static_year_span,static_year_time,delete_status,add_time,update_time,creator,update_user,publish,calc_status,statics_sum,type,is_top,statics_out_formula,statics_out_formula_new");
        p.setProperty("columns.types", "string:string:string:string:string:string:string:string:string:string:string:string:string:string:string:string:string:string:string:string:string:string:string");
        serde.initialize(conf, p);
        StructObjectInspector inspector = (StructObjectInspector) serde.getObjectInspector();
        InputFormat in = new OrcInputFormat();
        FileInputFormat.setInputPaths(conf, testFilePath.toString());
        InputSplit[] splits = in.getSplits(conf, 1);
        System.out.println("splits.length==" + splits.length);

        conf.set("hive.io.file.readcolumn.ids", "1");
        RecordReader reader = in.getRecordReader(splits[0], conf, Reporter.NULL);
        Object key = reader.createKey();
        Object value = reader.createValue();
        List<? extends StructField> fields = inspector.getAllStructFieldRefs();
        long offset = reader.getPos();
        while (reader.next(key, value)) {
            Object url = inspector.getStructFieldData(value, fields.get(0));
            Object word = inspector.getStructFieldData(value, fields.get(1));
            Object freq = inspector.getStructFieldData(value, fields.get(2));
            Object weight = inspector.getStructFieldData(value, fields.get(3));
            offset = reader.getPos();
            System.out.println(url + "|" + word + "|" + freq + "|" + weight);
        }
        reader.close();

    }

}
