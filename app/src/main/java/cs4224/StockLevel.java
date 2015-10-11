package cs4224;
import com.datastax.driver.core.*;

public class StockLevel {
    private Session session;
    private PreparedStatement selectNextOIdQuery;
    private PreparedStatement selectOlIIdQuery;
    private PreparedStatement selectSQuantityQuery;

    public static void main(String[] args) {
        int inputWId = 1, inputDId = 1, inputT = 1000, inputL = 20;
        StockLevel s = new StockLevel(new SimpleClient());
        s.executeQuery(inputWId, inputDId, inputT, inputL);
    }

    public StockLevel(SimpleClient client) {
        this.session = client.getSession();
        this.selectNextOIdQuery = session.prepare("SELECT d_next_o_id FROM Districts where d_w_id = ? AND d_id = ?;");
        this.selectOlIIdQuery = session.prepare("SELECT ol_i_id FROM OrderLines where ol_w_id = ? AND ol_d_id = ?" +
                " AND ol_o_id < ? AND ol_o_id >= ?;");
        this.selectSQuantityQuery = session.prepare("SELECT s_quantity FROM Stocks where s_w_id = ? AND s_i_id = ?;");
    }

    public void executeQuery(int inputWId, int inputDId, int inputT, int inputL) {
        ResultSet results = session.execute(selectNextOIdQuery.bind(inputWId, inputDId));
        int N = 0, M = 0;
        for (Row row : results) {
            N = row.getInt("d_next_o_id");
            M = N - inputL;
            break;
        }

        results = session.execute(selectOlIIdQuery.bind(inputWId, inputDId, N, M));
        int olIId = 0, sum = 0;
        for (Row row : results) {
            olIId = row.getInt("ol_i_id");
            ResultSet results1 = session.execute(selectSQuantityQuery.bind(inputWId, olIId));
            for (Row row1 : results1) {
                sum += (row1.getInt("s_quantity") < inputT) ? 1 : 0 ;
            }
        }
        System.out.format("Counts of items: %d", sum);
    }
}
