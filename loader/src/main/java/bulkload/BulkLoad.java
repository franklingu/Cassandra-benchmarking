/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bulkload;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Usage: java bulkload.BulkLoad
 */
public class BulkLoad
{
    /** Default output directory */
    public static final String DEFAULT_OUTPUT_DIR = "./../import";
    public static final String DEFAULT_DATA_DIR = "./../data";

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /** Keyspace name */
    public static final String KEYSPACE = "cs4224";

    /**
     * Schema for bulk loading table.
     * It is important not to forget adding keyspace name before table name,
     * otherwise CQLSSTableWriter throws exception.
     */
    public static final String SCHEMA1 = "CREATE TABLE cs4224.customers (" +
            "C_W_ID int," +
            "C_D_ID int," +
            "C_ID int," +
            "C_FIRST varchar," +
            "C_MIDDLE varchar," +
            "C_LAST varchar," +
            "C_STREET_1 varchar," +
            "C_STREET_2 varchar," +
            "C_CITY varchar," +
            "C_STATE varchar," +
            "C_ZIP varchar," +
            "C_PHONE varchar," +
            "C_SINCE timestamp," +
            "C_CREDIT varchar," +
            "C_CREDIT_LIM float," +
            "C_DISCOUNT float," +
            "C_BALANCE float," +
            "C_YTD_PAYMENT float," +
            "C_PAYMENT_CNT int," +
            "C_DELIVERY_CNT int," +
            "C_DATA varchar," +
            "PRIMARY KEY (C_W_ID, C_D_ID, C_ID));";

    public static final String SCHEMA2 = "CREATE TABLE cs4224.orders (" +
            "O_W_ID int," +
            "O_D_ID int," +
            "O_ID int," +
            "O_C_ID int," +
            "O_CARRIER_ID int," +
            "O_OL_CNT int," +
            "O_ALL_LOCAL int," +
            "O_ENTRY_D timestamp," +
            "PRIMARY KEY (O_W_ID, O_D_ID, O_ID));";


    public static final String SCHEMA3 = "CREATE TABLE cs4224.items (" +
            "I_ID int," +
            "I_NAME varchar," +
            "I_PRICE float," +
            "I_IM_ID int," +
            "I_DATA varchar," +
            "PRIMARY KEY (I_ID));";

    public static final String SCHEMA4 = "CREATE TABLE cs4224.orderLines (" +
            "OL_W_ID int," +
            "OL_D_ID int," +
            "OL_O_ID int," +
            "OL_NUMBER int," +
            "OL_I_ID int," +
            "OL_DELIVERY_D timestamp," +
            "OL_AMOUNT float," +
            "OL_SUPPLY_W_ID int," +
            "OL_QUANTITY int," +
            "OL_DIST_INFO varchar," +
            "PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER));";

    public static final String SCHEMA5 = "CREATE TABLE cs4224.stocks (" +
            "S_W_ID int," +
            "S_I_ID int," +
            "S_QUANTITY int," +
            "S_YTD float," +
            "S_ORDER_CNT int," +
            "S_REMOTE_CNT int," +
            "S_DIST_01 varchar," +
            "S_DIST_02 varchar," +
            "S_DIST_03 varchar," +
            "S_DIST_04 varchar," +
            "S_DIST_05 varchar," +
            "S_DIST_06 varchar," +
            "S_DIST_07 varchar," +
            "S_DIST_08 varchar," +
            "S_DIST_09 varchar," +
            "S_DIST_10 varchar," +
            "S_DATA text," +
            "PRIMARY KEY (S_W_ID, S_I_ID));";

    /**
     * INSERT statement to bulk load.
     * It is like prepared statement. You fill in place holder for each data.
     */
    public static final String INSERT_STMT1 = "INSERT INTO cs4224.customers (" +
            "c_w_id, c_d_id, c_id, c_first, c_middle, c_last,c_street_1, c_street_2, c_city, " +
            "c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, " +
            "c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data " +
            ") VALUES (" +
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?" +
            ");";

    public static final String INSERT_STMT2 = "INSERT INTO cs4224.orders (" +
            "o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d" +
            ") VALUES (" +
            " ?, ?, ?, ?, ?, ?, ?, ?" +
            ");";

    public static final String INSERT_STMT3 = "INSERT INTO cs4224.items (" +
            "i_id, i_name, i_price, i_im_id, i_data" +
            ") VALUES (" +
            " ?, ?, ?, ?, ?" +
            ");";


    public static final String INSERT_STMT4 = "INSERT INTO cs4224.orderlines (" +
            "ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info" +
            ") VALUES (" +
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?" +
            ");";

    public static final String INSERT_STMT5 = "INSERT INTO cs4224.stocks (" +
            "s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_data" +
            ") VALUES (" +
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?" +
            ");";

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("usage: Please specify the data set to use: e.g. -Pdataset=D8");
            return;
        } else {
            File outputDir = new File(DEFAULT_DATA_DIR + File.separator + args[0]);
            if (!outputDir.exists()) {
                throw new RuntimeException("No such data directory: " + outputDir);
            }
        }

        String datasetToUse = args[0];
        System.out.println("\n\n\n\nUsing dataset " + datasetToUse + "\n\n\n\n");
        // magic!
        Config.setClientMode(true);
        String[] fileNames = {"customer", "order", "item", "order-line", "stock"};
        String[] tableNames = {"customers", "orders", "items", "orderlines", "stocks"};
        String[] schemaTable = {SCHEMA1,SCHEMA2, SCHEMA3, SCHEMA4, SCHEMA5};
        String[] insertStatementTable = {INSERT_STMT1, INSERT_STMT2, INSERT_STMT3, INSERT_STMT4, INSERT_STMT5};


        for (int i=0;i<fileNames.length;i++) {

            String fileName = fileNames[i];
            String tableName = tableNames[i];

            // Create output directory that has keyspace and table name in the path
            File outputDir = new File(DEFAULT_OUTPUT_DIR + File.separator + KEYSPACE + File.separator + tableName);
            if (!outputDir.exists() && !outputDir.mkdirs()) {
                throw new RuntimeException("Cannot create output directory: " + outputDir);
            }

            // Prepare SSTable writer
            CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
            // set output directory
            builder.inDirectory(outputDir)
                    // set target schema
                    .forTable(schemaTable[i])
                            // set CQL statement to put data
                    .using(insertStatementTable[i]);

            CQLSSTableWriter writer = builder.build();

            try (
                BufferedReader reader = new BufferedReader(new FileReader(DEFAULT_DATA_DIR + File.separator + datasetToUse + File.separator + fileName + ".csv"));
                CsvListReader csvReader = new CsvListReader(reader, CsvPreference.STANDARD_PREFERENCE)
            ) {
                // Write to SSTable while reading data
                List<String> line;
                while ((line = csvReader.read()) != null) {
                    writer.addRow(generateArgument(i, line));
                }
            }
            catch (InvalidRequestException | IOException e) {
                e.printStackTrace();
            }

            try {
                writer.close();
            }
            catch (IOException ignore) {}
        }
    }


    public static List<Object> generateArgument(int tableIndex, List<String> line) {
        List<Object> args = new ArrayList<>();
        switch (tableIndex) {
            case 0: // Customers
                args.add(Integer.parseInt(line.get(0)));
                args.add(Integer.parseInt(line.get(1)));
                args.add(Integer.parseInt(line.get(2)));
                args.add(line.get(3));
                args.add(line.get(4));
                args.add(line.get(5));
                args.add(line.get(6));
                args.add(line.get(7));
                args.add(line.get(8));
                args.add(line.get(9));
                args.add(line.get(10));
                args.add(line.get(11));
                Date arg12;
                try {
                    arg12 = DATE_FORMAT.parse(line.get(12));
                } catch (ParseException e) {
                    arg12 = null;
                }
                args.add(arg12);
                args.add(line.get(13));
                args.add(Float.parseFloat(line.get(14)));
                args.add(Float.parseFloat(line.get(15)));
                args.add(Float.parseFloat(line.get(16)));

                args.add(Float.parseFloat(line.get(17)));

                args.add(Integer.parseInt(line.get(18)));
                args.add(Integer.parseInt(line.get(19)));
                args.add(line.get(20));
                break;
            case 1: // Orders
                args.add(Integer.parseInt(line.get(0)));
                args.add(Integer.parseInt(line.get(1)));
                args.add(Integer.parseInt(line.get(2)));
                args.add(Integer.parseInt(line.get(3)));

                Integer arg4; // Carrier Id: null-able
                try {
                    arg4 = Integer.parseInt((line.get(4)));
                } catch (NumberFormatException e) {
                    arg4 = 0;
                }
                args.add(arg4);

                args.add(Integer.parseInt(line.get(5)));  // o_ol_cnt
                args.add(Integer.parseInt(line.get(6)));  // o_all_local
                Date arg7;
                try {
                    arg7 = DATE_FORMAT.parse(line.get(7));
                } catch (ParseException e) {
                    arg7 = null;
                }
                args.add(arg7);
                break;
            case 2: // Items
                args.add(Integer.parseInt(line.get(0)));
                args.add(line.get(1));
                args.add(Float.parseFloat(line.get(2)));
                args.add(Integer.parseInt(line.get(3)));
                args.add(line.get(4));
                break;
            case 3: // OrderLines
                args.add(Integer.parseInt(line.get(0)));
                args.add(Integer.parseInt(line.get(1)));
                args.add(Integer.parseInt(line.get(2)));
                args.add(Integer.parseInt(line.get(3)));
                args.add(Integer.parseInt(line.get(4)));
                Date arg5;
                try {
                    arg5 = DATE_FORMAT.parse(line.get(5));
                } catch (ParseException e) {
                    arg5 = null;
                }
                args.add(arg5);
                args.add(Float.parseFloat(line.get(6)));
                args.add(Integer.parseInt(line.get(7)));
                args.add(Integer.parseInt(line.get(8)));  // ol_quantity
                args.add(line.get(9));
                break;
            case 4: // Stocks
                args.add(Integer.parseInt(line.get(0)));
                args.add(Integer.parseInt(line.get(1)));

                args.add(Integer.parseInt(line.get(2)));  // s_quantity
                args.add(Float.parseFloat(line.get(3)));

                args.add(Integer.parseInt(line.get(4)));
                args.add(Integer.parseInt(line.get(5)));

                args.add(line.get(6));
                args.add(line.get(7));
                args.add(line.get(8));
                args.add(line.get(9));
                args.add(line.get(10));
                args.add(line.get(11));
                args.add(line.get(12));
                args.add(line.get(13));
                args.add(line.get(14));
                args.add(line.get(15));

                args.add(line.get(16));
                break;
        }

        return args;
    }
}
