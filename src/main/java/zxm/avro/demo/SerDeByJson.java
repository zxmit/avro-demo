package zxm.avro.demo;

import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import org.apache.avro.generic.GenericData.Record;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created by zxm on 2015/10/13.
 */
public class SerDeByJson {

    /**
     * 将Json转换为schema
     *
     * @param json
     * @return
     */
    public static Schema json2Schema(String json) {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(json);
        return schema;
    }

    /**
     * 将数据objects对象转换为avro，并将avro转换为bytes数组
     *
     * @param schema
     * @param objects
     * @return
     * @throws IOException
     */
    public static byte[] serialize(Schema schema, Object[] objects) throws IOException {

        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericRecord record = new GenericData.Record(schema);
        for (int i = 0; i < objects.length; ++i) {
            record.put(i, objects[i]);
        }
        writer.write(record, encoder);
        encoder.flush();
        out.close();
        return out.toByteArray();
    }

    /**
     * 数据转换，使输入的数据与schmea格式一致
     *
     * @param payload
     * @return
     */
    public static Object[] deserialize(Schema schema, byte[] payload) throws IOException {
        DatumReader<Record> reader = new GenericDatumReader<Record>(schema);
        Record record = reader.read(null, DecoderFactory.get().binaryDecoder(payload, 0, payload.length, null));
        int size = record.getSchema().getFields().size();
        Object[] objects = new Object[size];
        for (int i = 0; i < size; i++) {
            objects[i] = record.get(i);
        }
        return objects;
    }

    /**
     * 将数组对象序列化为AVRO数组
     * @param schema
     * @param objectss
     * @return
     */
    public static byte[] serializeArray(Schema schema, List<Object[]> objectss) throws IOException {
        Schema schemaArr = Schema.createArray(schema);
        GenericArray<GenericRecord> recordArray = new GenericData.Array<GenericRecord>(objectss.size(), schemaArr);
        for (Object[] objects : objectss) {
            GenericRecord record = new GenericData.Record(schema);
            for (int i = 0; i < objects.length; ++i) {
                record.put(i, objects[i]);
            }
            recordArray.add(record);
        }
        DatumWriter<GenericArray<GenericRecord>> writer = new GenericDatumWriter<GenericArray<GenericRecord>>(schemaArr);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(recordArray, encoder);
        encoder.flush();
        out.close();
        return out.toByteArray();
    }

    /**
     * 反序列化AVRO数组
     * @param schema
     * @param payload
     * @return
     * @throws IOException
     */
    public static List<Object[]> deserializeArray(Schema schema, byte[] payload) throws IOException {
        Schema schemaArray = Schema.createArray(schema);
        DatumReader<GenericArray> reader = new GenericDatumReader<GenericArray>(
                schemaArray);
        GenericArray<Record> array = reader.read(null, DecoderFactory.get().binaryDecoder(payload, null));
        Iterator<Record> recordit = array.iterator();
        List<Object[]> results = new ArrayList<Object[]>();
        while (recordit.hasNext()) {

            Record record = recordit.next();
            int size = record.getSchema().getFields().size();
            Object[] objects = new Object[size];
            for (int i = 0; i < size; i++) {
                objects[i] = record.get(i);
            }
            results.add(objects);
        }
        return results;
    }

    public static void main(String[] args) throws IOException {
        String json = "{\n" +
                "  \"namespace\": \"example.avro\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"User\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"name\", \"type\": \"string\"},\n" +
                "    {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" +
                "    {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n" +
                "  ]\n" +
                "}";
        Schema schema = json2Schema(json);
        Object[] obj1 = new Object[]{"xm", 13, "blue"};
        Object[] obj2 = new Object[]{"xm", 14, "blue"};
        Object[] obj3 = new Object[]{"xm", 15, "blue"};
        Object[] obj4 = new Object[]{"xm", 13, "blue"};

        System.out.println("*************************** serialize single record ***************************");
        byte[] bytes = serialize(schema, obj1);
        Object[] objects = deserialize(schema, bytes);
        for(Object obj : objects) {
            System.out.print(obj.toString() + "  ");
        }
        System.out.println();

        System.out.println("*************************** serialize record array ***************************");
        List<Object[]> objectss = new ArrayList<Object[]>();
        objectss.add(obj1);
        objectss.add(obj2);
        objectss.add(obj3);
        objectss.add(obj4);
        byte[] bytesArr = serializeArray(schema, objectss);
        List<Object[]> results = deserializeArray(schema, bytesArr);
        for(Object[] objs : results) {
            for(Object obj : objs) {
                System.out.print(obj.toString() + "  ");
            }
            System.out.println();
        }
    }
}
