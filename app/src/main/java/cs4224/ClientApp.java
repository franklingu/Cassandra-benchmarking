package cs4224;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class ClientApp {
    private boolean useD8 = true;

    public static void main( String[] args ) {
        boolean useD8;
        if (args == null || args.length <= 0) {
            useD8 = true;
        } else {
            useD8 = args[0] == "D8";
        }
        ClientApp ca = new ClientApp(useD8);
        ca.runQueries();
    }

    public  ClientApp(boolean useD8) {
        this.useD8 = useD8;
    }

    public void runQueries() {
        // Initialize connector
        SimpleClient client = new SimpleClient();
        client.connect("127.0.0.1", "cs4224");

        // Initialize transaction
        NewOrder n = new NewOrder(client);
        OrderStatus o = new OrderStatus(client);
        Delivery d = new Delivery(client);
        StockLevel s = new StockLevel(client);
        Payment p = new Payment(client);
        PopularItem popular = new PopularItem(client);

        String pathTemplate = "../data/xact-spec-files/D%d-xact-files/%d.txt";
        long startTime = System.nanoTime();
        int totalTransactions = 0;
        for (int i = 0; i < 100; i++) {
            File file = new File(String.format(pathTemplate, useD8 ? 8 : 40, i));
            try {
                BufferedReader reader = new BufferedReader(new FileReader(file));
                String inputLine = reader.readLine();
                while (inputLine != null && inputLine.length() > 0) {
                    String[] params = inputLine.split(",");
                    if (inputLine.charAt(0) == 'N') {
                        int cId = Integer.parseInt(params[1]);
                        int wId = Integer.parseInt(params[2]);
                        int dId = Integer.parseInt(params[3]);
                        int numItems = Integer.parseInt(params[4]);
                        int[] itemNumbers = new int[numItems];
                        int[] supplierWarehouse = new int[numItems];
                        int[] quantity = new int[numItems];

                        String newLine;
                        String[] newParams;
                        for (int j = 0; j < numItems; j++) {
                            newLine = reader.readLine();
                            newParams = newLine.split(",");
                            itemNumbers[j] = Integer.parseInt(newParams[0]);
                            supplierWarehouse[j] = Integer.parseInt(newParams[1]);
                            quantity[j] = Integer.parseInt(newParams[2]);
                        }
                        n.createOrder(wId, dId, cId, numItems, itemNumbers, supplierWarehouse, quantity);
                    } else if (inputLine.charAt(0) == 'P') {
                        int wId = Integer.parseInt(params[1]);
                        int dId = Integer.parseInt(params[2]);
                        int cId = Integer.parseInt(params[3]);
                        float payment = Float.parseFloat(params[4]);
                        p.processPayment(wId, dId, cId, payment);
                    } else if (inputLine.charAt(0) == 'D') {
                        int wId = Integer.parseInt(params[1]);
                        int carrierId = Integer.parseInt(params[2]);
                        d.executeQuery(wId, carrierId);
                    } else if (inputLine.charAt(0) == 'O') { // Order Status
                        int wId = Integer.parseInt(params[1]);
                        int dId = Integer.parseInt(params[2]);
                        int cId = Integer.parseInt(params[3]);
                        o.getOrderStatus(wId, dId, cId);
                    } else if (inputLine.charAt(0) == 'S') {
                        int wId = Integer.parseInt(params[1]);
                        int dId = Integer.parseInt(params[2]);
                        int T = Integer.parseInt(params[3]);
                        int L = Integer.parseInt(params[4]);
                        s.executeQuery(wId, dId, T, L);
                    } else if (inputLine.charAt(0) == 'I') {
                        int wId = Integer.parseInt(params[1]);
                        int dId = Integer.parseInt(params[2]);
                        int L = Integer.parseInt(params[3]);
                        popular.findItem(wId, dId, L);
                    } else {
                        System.out.println("\n\nSeems the way of reading of file is wrong\n\n");
                    }
                    totalTransactions++;
                    System.out.println(); // new line
                    inputLine = reader.readLine();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1000000000;
        System.err.println(String.format("Total Transactions: %d", totalTransactions));
        System.err.println(String.format("Time used: %d s", duration));
        System.err.println(String.format("Throughput: %.4f", totalTransactions / (float)duration));
        client.close();
    }
}
