package com.interview.app;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Scanner;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception
    {
        File file = new File("ms3Interview.csv");
        String ingestFileName = file.getName().replaceFirst("[.][^.]+$", "");
        Scanner sc = new Scanner(file);

        int rreceived = 0;
        int rsuccessful = 0;
        int rfailed = 0;

        PrintWriter writer = new PrintWriter(new File(ingestFileName + "-bad.csv"));
        StringBuilder badRecordsString = new StringBuilder();

        Connection connection = null;
        PreparedStatement ps = null;
        String query = "insert into records values(?,?,?,?,?,?,?,?,?,?)";
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:" + ingestFileName + ".db");

            Statement statement = connection.createStatement();
            statement.executeUpdate("drop table if exists records");
            statement.executeUpdate("create table records (A string, B string, C string, D string, E string, F string, G string, H string, I string, J string)");

            ps = connection.prepareStatement(query);

            sc.nextLine(); // to ignore first row which holds column names

            while(sc.hasNextLine()){
                String currentLine = sc.nextLine();
                String[] values = currentLine.replace("'", "''").split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

                if(values.length == 10){
                    // insert to db
                    ps.setString(1, values[0]);
                    ps.setString(2, values[1]);
                    ps.setString(3, values[2]);
                    ps.setString(4, values[3]);
                    ps.setString(5, values[4]);
                    ps.setString(6, values[5]);
                    ps.setString(7, values[6]);
                    ps.setString(8, values[7]);
                    ps.setString(9, values[8]);
                    ps.setString(10, values[9]);
                    ps.addBatch();

                    rsuccessful++;
                } else {
                    // add bad entries to csv
                    badRecordsString.append(currentLine);
                    badRecordsString.append("\\n");

                    rfailed++;
                }
                rreceived++;
            }

            writer.write(badRecordsString.toString());
            ps.executeBatch();
        } catch (SQLException e){
            // connection, insertion, or values failed
            System.err.println(e.getMessage());
        } finally {
            try {
                sc.close();
                writer.close();
                if (ps != null) ps.close();
                if (connection != null) connection.close();
            } catch (SQLException e) {
                // closing failed
                System.err.println(e.getMessage());
            }
        }

        logging(rreceived, rsuccessful, rfailed);
    }

    private static void logging(int received, int successful, int failed) {
        Logger logger = Logger.getLogger(App.class.getName());
        FileHandler fh = null;

        try{
            fh = new FileHandler("injestlog.log", true);

            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);

            logger.addHandler(fh);
        } catch (SecurityException | IOException e) {
            e.printStackTrace();
        }

        logger.info(received + " of records received");
        logger.info(successful + " of records successful");
        logger.info(failed + " of records failed");

        if(fh != null) fh.close();
    }
}
