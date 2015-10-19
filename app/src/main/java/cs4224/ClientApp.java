package cs4224;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class ClientApp {
    private boolean useD8 = true;
    private int transactionFileNumber = 0;

    public static void main( String[] args ) {
        boolean useD8;
        int transactionFileNumber;
        if (args == null || args.length <= 0) {
            useD8 = true;
            transactionFileNumber = 1;
        } else {
            useD8 = (args[0].trim()).equals("D8");
            transactionFileNumber = Integer.parseInt(args[1]);
        }

        ClientApp ca = new ClientApp(useD8, transactionFileNumber);
        ca.runQueries();
    }

    public ClientApp(boolean useD8, int transactionFileNumber) {
        this.useD8 = useD8;
        this.transactionFileNumber = transactionFileNumber;
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
        long[] timings = new long[6];
        int[] transactionCounts = new int[6];
        long startTime;
        File file = new File(String.format(pathTemplate, useD8 ? 8 : 40, transactionFileNumber));
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

                    try {
                        startTime = System.nanoTime();
                        n.createOrder(wId, dId, cId, numItems, itemNumbers, supplierWarehouse, quantity);
                        timings[0] = timings[0] + (System.nanoTime() - startTime);
                        transactionCounts[0] = transactionCounts[0] + 1;
                    } catch (Exception e) {
                        System.err.println(e.getStackTrace());
                    }
                } else if (inputLine.charAt(0) == 'P') {
                    int wId = Integer.parseInt(params[1]);
                    int dId = Integer.parseInt(params[2]);
                    int cId = Integer.parseInt(params[3]);
                    float payment = Float.parseFloat(params[4]);

                    try {
                        startTime = System.nanoTime();
                        p.processPayment(wId, dId, cId, payment);
                        timings[1] = timings[1] + (System.nanoTime() - startTime);
                        transactionCounts[1] = transactionCounts[1] + 1;
                    } catch (Exception e) {
                        System.err.println(e.getStackTrace());
                    }
                } else if (inputLine.charAt(0) == 'D') {
                    int wId = Integer.parseInt(params[1]);
                    int carrierId = Integer.parseInt(params[2]);

                    try {
                        startTime = System.nanoTime();
                        d.executeQuery(wId, carrierId);
                        timings[2] = timings[2] + (System.nanoTime() - startTime);
                        transactionCounts[2] = transactionCounts[2] + 1;
                    } catch (Exception e) {
                        System.err.println(e.getStackTrace());
                    }
                } else if (inputLine.charAt(0) == 'O') { // Order Status
                    int wId = Integer.parseInt(params[1]);
                    int dId = Integer.parseInt(params[2]);
                    int cId = Integer.parseInt(params[3]);

                    try {
                        startTime = System.nanoTime();
                        o.getOrderStatus(wId, dId, cId);
                        timings[3] = timings[3] + (System.nanoTime() - startTime);
                        transactionCounts[3] = transactionCounts[3] + 1;
                    } catch (Exception e) {
                        System.err.println(e.getStackTrace());
                    }
                } else if (inputLine.charAt(0) == 'S') {
                    int wId = Integer.parseInt(params[1]);
                    int dId = Integer.parseInt(params[2]);
                    int T = Integer.parseInt(params[3]);
                    int L = Integer.parseInt(params[4]);

                    try {
                        startTime = System.nanoTime();
                        s.executeQuery(wId, dId, T, L);
                        timings[4] = timings[4] + (System.nanoTime() - startTime);
                        transactionCounts[4] = transactionCounts[4] + 1;
                    } catch (Exception e) {
                        System.err.println(e.getStackTrace());
                    }
                } else if (inputLine.charAt(0) == 'I') {
                    int wId = Integer.parseInt(params[1]);
                    int dId = Integer.parseInt(params[2]);
                    int L = Integer.parseInt(params[3]);

                    try {
                        startTime = System.nanoTime();
                        popular.findItem(wId, dId, L);
                        timings[5] = timings[5] + (System.nanoTime() - startTime);
                        transactionCounts[5] = transactionCounts[5] + 1;
                    } catch (Exception e) {
                        System.err.println(e.getStackTrace());
                    }
                } else {
                    System.err.println("\n\nSeems the way of reading of file is wrong\n\n");
                }
                System.out.println(); // new line
                inputLine = reader.readLine();
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        client.close();

        float totalTiming = (float)0.0;
        float duration;
        float throughput;
        int totalCounts = 0;
        for (int i = 0; i < 6; i++) {
            if (transactionCounts[i] == 0) {
                continue;
            }
            duration = (float)timings[i] / 1000000000;
            throughput = (float)(transactionCounts[i]) / duration;
            totalTiming = totalTiming + duration;
            totalCounts = totalCounts + transactionCounts[i];
            System.err.println(String.format("Type %d: Total Transactions: %d", i, transactionCounts[i]));
            System.err.println(String.format("Type %d: Time used: %f s", i, duration));
            System.err.println(String.format("Type %d: Throughput: %f", i, throughput));
        }
        throughput = (float)totalCounts / totalTiming;
        System.err.println(String.format("Overall: Total Transactions: %d", totalCounts));
        System.err.println(String.format("Overall: Time used: %f s", totalTiming));
        System.err.println(String.format("Overall: Throughput: %f", throughput));
    }
}
