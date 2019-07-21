import com.github.javafaker.Faker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hive.orc.TypeDescription;

import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;


class OrcUtilities {
     void ReadOrcFile(Path path, Configuration conf) throws Exception {
        try {
            OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf);
            Reader reader = OrcFile.createReader(path, readerOptions);
            StructObjectInspector inspector = (StructObjectInspector) reader.getObjectInspector();
            System.out.println("Compression Size: " + reader.getCompressionSize());
            RecordReader records = reader.rows();
            Object row = null;
            int loopCount = 0;
            List fields = inspector.getAllStructFieldRefs();
            for (int i = 0; i < fields.size(); ++i) {
                System.out.print(((StructField) fields.get(i)).getFieldObjectInspector().getTypeName() + '\t'+"\n");
            }
            while (records.hasNext()) {
                row = records.next(row);
                List value_lst = inspector.getStructFieldsDataAsList(row);
                StringBuilder builder = new StringBuilder();
                for (Object field : value_lst) {
                    if (field != null)
                        builder.append(field.toString());
                    builder.append('|');
                }
                System.out.println(builder.toString());
                loopCount++;
                if (loopCount % 10 == 0) {
                    System.out.println("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
                    System.out.println("Maximum Memory: " + RuntimeMemoryUtil.getMaxMemoryInMB());
                    System.out.println("Total Available Memory: " + RuntimeMemoryUtil.getTotalMemoryInMB());
                    System.out.println("Memory used: " + RuntimeMemoryUtil.getUsedMemoryInMB());
                    System.out.println("Percentage of memory used: " + RuntimeMemoryUtil.getPercentageUsedFormatted());
                    System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

     void WriteOrcFile(Path path, Configuration conf) throws Exception {
        try {
            //Instantiating Faker class and required methods
            Faker faker = new Faker();
            
            //Define the schema of the data you want to generate
            TypeDescription schema = TypeDescription.fromString("struct<id:bigint,col_1:bigint," +
                    "col_2:bigint,col_3:bigint," +
                    "col_4:bigint,col_5:bigint,col_6:bigint>");

            //Orc Writer
            OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf).setSchema(schema);
            Writer writer = OrcFile.createWriter(path, writerOptions);
            VectorizedRowBatch batch = schema.createRowBatch();

            LongColumnVector id = (LongColumnVector) batch.cols[0];
            LongColumnVector col_1 = (LongColumnVector) batch.cols[1];
            LongColumnVector col_2 = (LongColumnVector) batch.cols[2];
            LongColumnVector col_3 = (LongColumnVector) batch.cols[3];
            LongColumnVector col_4 = (LongColumnVector) batch.cols[4];
            LongColumnVector col_5 = (LongColumnVector) batch.cols[5];
            LongColumnVector col_6 = (LongColumnVector) batch.cols[6];
           
            for (int r = 0; r < 100; ++r) {
                int row = batch.size++;
                id.vector[row] = r;
                col_1.vector[row] = Math.abs(faker.random().nextLong(99999999999L));
                col_2.vector[row] = Math.abs(faker.random().nextLong(99999999999L));
                col_3.vector[row] = Math.abs(faker.random().nextLong(99999999999L));
                col_4.vector[row] = Math.abs(faker.random().nextLong(99999999999L));
                col_5.vector[row] = Math.abs(faker.random().nextLong(99999999999L));
                col_6.vector[row] = Math.abs(faker.random().nextLong(99999999999L));
                
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
            System.out.println("ORC file generated");
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
