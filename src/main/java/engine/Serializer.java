package engine;

import java.io.*;

public class Serializer {
    public static void serializeTo(Object object, String filePath) throws DBAppException {
        try {
            FileOutputStream file = new FileOutputStream(filePath);
            ObjectOutputStream out = new ObjectOutputStream(file);

            out.writeObject(object);

            out.close();
            file.close();
        } catch (Exception ex) {
            throw new DBAppException("Failed to serialize");
        }
    }

    public static <T> T deserializeFrom(String filePath) throws DBAppException {
        T result = null;

        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filePath);
            ObjectInputStream in = new ObjectInputStream(file);

            result = (T) in.readObject();

            in.close();
            file.close();

        }

        catch (Exception ex) {
            throw new DBAppException("Failed to deserialize");
        }

        return result;
    }
}
