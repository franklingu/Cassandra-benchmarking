package cs4224;
import com.datastax.driver.core.*;

public class StockLevel {
    public static void main(String[] args) {
        int inputWId = 1, inputDId = 1, inputT = 1000, inputL = 20;
        StockLevel.executeQuery(inputWId, inputDId, inputT, inputL);
    }

    public static void executeQuery(int inputWId, int inputDId, int inputT, int inputL) {
        Cluster cluster;
        Session session;

        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("cs4224");

        String query = String.format("SELECT d_next_o_id FROM Districts where d_w_id = %d AND d_id = %d",
                inputWId, inputDId);
        ResultSet results = session.execute(query);
        int N = 0, M = 0;
        for (Row row : results) {
            N = row.getInt("d_next_o_id");
            M = N - 20;
            System.out.format("N: %d\n", N);
            break;
        }

        query = String.format("SELECT ol_i_id FROM OrderLines where ol_w_id = %d AND ol_d_id = %d" +
                        " AND ol_o_id < %d AND ol_o_id >= %d", inputWId, inputDId, N, M);
        results = session.execute(query);
        int olIId = 0, sum = 0;
        for (Row row : results) {
            olIId = row.getInt("ol_i_id");
            System.out.format("OL_I_ID: %d\n", olIId);
            query = String.format("SELECT s_quantity FROM Stocks where s_w_id = %d AND s_i_id = %d",
                    inputWId, olIId);
            ResultSet results1 = session.execute(query);
            for (Row row1 : results1) {
                sum += (row1.getInt("s_quantity") < inputT) ? 1 : 0 ;
            }
        }
        System.out.format("Counts: %d", sum);

        cluster.close();
    }
}
