// This program tests the basic functionality of the database system.
// It creates a table, inserts values into it, and then queries that data
// and ensures that all of it was inserted into the database.

import java.sql.*;
import simpledb.remote.SimpleDriver;

public class EndToEnd {
    public static final String CHECKMARK = "\u2713 ";

    public static void main(String[] args) {

        Connection connection = null;

        try {
            // Connect to the database server.
            Driver driver = new SimpleDriver();
            connection = driver.connect("jdbc:simpledb://localhost", null);
            Statement statement = connection.createStatement();

            // Delete the cars table so we can start with a blank slate.
            String sql = "delete from cars(id, year, make, model)";
            System.out.println(sql);
            statement.executeUpdate(sql);

            // Create cars table;
            sql = "create table cars(id int, year int, make varchar(15), model varchar(15))";
            System.out.println(sql);
            statement.executeUpdate(sql);

            // Create data.
            String[][] cars = {
                {"1", "2006", "Toyota", "Camry"},
                {"2", "2009", "Kia", "Optima"},
                {"3", "1995", "Honda", "Accord"},
                {"4", "2004", "Toyota", "Corolla"}
            };

            for(int i = 0; i < cars.length; i++) {
                sql = "insert into cars(id, year, make, model) values (";
                sql = sql + cars[i][0] + ", ";
                sql = sql + cars[i][1] + ", ";
                sql = sql + "'" + cars[i][2] + "', ";
                sql = sql + "'" + cars[i][3] + "')";

                System.out.println(sql);
                statement.executeUpdate(sql);
            }

            // Query the entire car table.
            sql = "select id, year, make, model from cars";
            ResultSet resultSet = statement.executeQuery(sql);
            System.out.println("\nID\tYear\tMake\tModel");
            int index = 0;
            boolean correct = true;

            // Go through each result and check it against the cars table.
            while(resultSet.next()) {
                int id = resultSet.getInt("id");
                int year = resultSet.getInt("year");
                String make = resultSet.getString("make");
                String model = resultSet.getString("model");

                // Check if the result matches the entry from the original table.
                if(id != Integer.parseInt(cars[index][0]) || year != Integer.parseInt(cars[index][1]) || !make.equals(cars[index][2]) || !model.equals(cars[index][3])) {
                    correct = false;
                }
                System.out.println(id + "\t" + year + "\t" + make + "\t" + model);
                index++;
            }

            // Check to make sure the query returned the correct number of records.
            if(index != 4) { correct = false; }

            if(correct == true) {
                System.out.println(CHECKMARK + " Successfully queries entire table.");
            } else {
                System.out.println("X The results of querying the entire table were incorrect.");
            }
            resultSet.close();

            // Execute a query with a conditional clause.
            sql = "select id, year, make, model from cars where make = 'Toyota'";
            resultSet = statement.executeQuery(sql);
            System.out.println("\nID\tYear\tMake\tModel");
            index = 0;
            correct = true;

            while(resultSet.next()) {
                int id = resultSet.getInt("id");
                int year = resultSet.getInt("year");
                String make = resultSet.getString("make");
                String model = resultSet.getString("model");

                if(!make.equals("Toyota")) {
                    correct = false;
                }

                System.out.println(id + "\t" + year + "\t" + make + "\t" + model);
                index++;
            }

            // Check to make sure the query returned the correct number of results.
            if(index != 2) { correct = false; }

            if(correct == true) {
                System.out.println(CHECKMARK + "Successfully queries the table with a condition");
            } else {
                System.out.println("X The results of queryint the table with a condition were incorrect.");
            }
            resultSet.close();

        } catch(SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if(connection != null) { connection.close(); }
            } catch(SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
