import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class TestHive {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        // Register driver and create driver instance
        Class.forName(driverName);

        // get connection
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "APP", "mine");

        // create statement
        Statement stmt = con.createStatement();

        // execute statement
//        stmt.executeQuery("CREATE TABLE tbl_epls (name STRING, club STRING, age INT, position STRING, market_value FLOAT, nationality STRING)   ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  STORED AS TEXTFILE");
//        stmt.executeQuery("LOAD DATA LOCAL INPATH '/Users/Storage/Vicohub/Python/practice/epldata_shortlist.csv' OVERWRITE INTO TABLE tbl_epls");
//        System.out.println("Load Data into employee successful");
//        stmt.executeQuery("DROP TABLE IF EXISTS tbl_epls");
        ResultSet res = stmt.executeQuery("SELECT * FROM tbl_epls");

        while (res.next())
        {
            System.out.println(res.getString(1) + "\t"
                    + res.getString(2) + "\t"
                    + res.getInt(3) + "\t"
                    + res.getString(4) + "\t"
                    + res.getDouble(5) + "\t"
                    + res.getString(6));
        }
        con.close();
}
}
