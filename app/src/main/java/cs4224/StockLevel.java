package cs4224;
import com.datastax.driver.core.*;

public class StockLevel {
    public static void main(String[] args) {
        int inputWId = 1, inputDId = 1, inputT = 10, inputL = 20;
        StockLevel.executeQuery(inputWId, inputDId, inputT, inputL);
    }

    public static void executeQuery(int inputWId, int inputDId, int inputT, int inputL) {
        Cluster cluster;
        Session session;

        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("cs4224");


        cluster.close();
    }
}
