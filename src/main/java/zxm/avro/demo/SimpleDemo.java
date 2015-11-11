package zxm.avro.demo;

import example.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * Created by zxm on 2015/10/12.
 */
public class SimpleDemo {

    /**
     * 创建User对象并进行序列化
     */
    public static void serialize() throws IOException {
        User user = new User();
        User user1 = new User("Buill", 123, "red");
        User user2 = new User();
        user2.setName("Alyssa");
        user2.setFavoriteNumber(321);
        User user3 = User.newBuilder()
                .setName("Charlie")
                .setFavoriteColor(null)
                .setFavoriteNumber(111)
                .build();

        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        dataFileWriter.create(user1.getSchema(), new File("users.avro"));
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();
    }

    /**
     * 反序列化
     */
    public static void deserilize() throws IOException {
        DatumReader<User> dataReader = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<User>(new File("users.avro"), dataReader);
        GenericRecord user = null;
        while(dataFileReader.hasNext()) {
            user = dataFileReader.next();
            System.out.println(user);
        }
    }

    public static void main(String[] args) throws IOException {
        deserilize();
    }
}
