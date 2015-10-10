package cs4224;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class ClientApp {
    public static void main( String[] args ) {
        File file = new File("../data/xact-spec-files/D8-xact-files/0.txt");
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String inputLine = reader.readLine();
            while (inputLine != null && inputLine.length() > 0) {
                String[] params = inputLine.split(",");
                if (inputLine.charAt(0) == 'N') {
                    int wId = Integer.parseInt(params[1]);
                    int dId = Integer.parseInt(params[2]);
                    int cId = Integer.parseInt(params[3]);
                    int numItems = Integer.parseInt(params[4]);
                    int[][] infos = new int[numItems][3];
                    for (int i = 0; i < numItems; i++) {
                        String newLine = reader.readLine();
                        String[] newParams = newLine.split(",");
                        infos[i] = new int[]{Integer.parseInt(newParams[0]), Integer.parseInt(newParams[1]),
                                Integer.parseInt(newParams[2])};
                    }
                } else if (inputLine.charAt(0) == 'P') {
                    int wId = Integer.parseInt(params[1]);
                    int dId = Integer.parseInt(params[2]);
                    float payment = Float.parseFloat(params[3]);
                } else if (inputLine.charAt(0) == 'D') {
                    int wId = Integer.parseInt(params[1]);
                    int carrierId = Integer.parseInt(params[2]);
                } else if (inputLine.charAt(0) == 'O') {
                    int wId = Integer.parseInt(params[1]);
                    int dId = Integer.parseInt(params[2]);
                    int cId = Integer.parseInt(params[3]);
                } else if (inputLine.charAt(0) == 'S') {
                    int wId = Integer.parseInt(params[1]);
                    int dId = Integer.parseInt(params[2]);
                    int T = Integer.parseInt(params[3]);
                    int L = Integer.parseInt(params[4]);
                } else if (inputLine.charAt(0) == 'I') {
                    int wId = Integer.parseInt(params[1]);
                    int dId = Integer.parseInt(params[2]);
                    int L = Integer.parseInt(params[3]);
                } else {
                    System.out.println("\n\nSeems the way of reading of file is wrong\n\n");
                }
                inputLine = reader.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
