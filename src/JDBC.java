import java.sql.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class JDBC {
    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/movieratings_db";
        String username = "root";
        String password = ""; // XAMPP default password is empty

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(url, username, password);
            System.out.println("✅ Connected to MariaDB successfully!");

            createTables(connection);
            importCSVData(connection);

            connection.close();
            System.out.println("✅ All done! Tables created and CSV data imported.");

        } catch (SQLException e) {
            System.out.println("❌ SQL Error: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.out.println("❌ Driver not found: " + e.getMessage());
        }
    }

    // -----------------------------
    // CREATE TABLES
    // -----------------------------
    private static void createTables(Connection connection) throws SQLException {
        String createUsers = "CREATE TABLE IF NOT EXISTS users (" +
                "user_id VARCHAR(10) PRIMARY KEY, " +
                "name VARCHAR(255)" +
                ")";

        String createMovies = "CREATE TABLE IF NOT EXISTS movies (" +
                "movie_id VARCHAR(10) PRIMARY KEY, " +
                "title VARCHAR(512), " +
                "genres VARCHAR(255)" +
                ")";

        // ✅ FIXED: Added `rating_id` column to match your CSV file
        String createRatings = "CREATE TABLE IF NOT EXISTS ratings (" +
                "rating_id VARCHAR(10) PRIMARY KEY, " +
                "user_id VARCHAR(10), " +
                "movie_id VARCHAR(10), " +
                "rating DOUBLE, " +
                "FOREIGN KEY (user_id) REFERENCES users(user_id), " +
                "FOREIGN KEY (movie_id) REFERENCES movies(movie_id)" +
                ")";

        Statement stmt = connection.createStatement();
        stmt.execute(createUsers);
        stmt.execute(createMovies);
        stmt.execute(createRatings);
        stmt.close();

        System.out.println("✅ Tables created (if not already).");
    }

    // -----------------------------
    // IMPORT CSV FILES
    // -----------------------------
    private static void importCSVData(Connection connection) {
        importUsers(connection, "users.csv");
        importMovies(connection, "movies.csv");
        importRatings(connection, "ratings.csv");
    }

    private static void importUsers(Connection connection, String filePath) {
        String sql = "INSERT IGNORE INTO users (user_id, name) VALUES (?, ?)";
        try (BufferedReader br = new BufferedReader(new FileReader(filePath));
             PreparedStatement pstmt = connection.prepareStatement(sql)) {
            br.readLine(); // Skip header
            String line;
            while ((line = br.readLine()) != null) {
                String[] data = line.split(",");
                if (data.length >= 2) {
                    pstmt.setString(1, data[0]);
                    pstmt.setString(2, data[1]);
                    pstmt.executeUpdate();
                }
            }
            System.out.println("✅ Imported users.csv");
        } catch (IOException | SQLException e) {
            System.out.println("⚠️ Error importing users.csv: " + e.getMessage());
        }
    }

    private static void importMovies(Connection connection, String filePath) {
        String sql = "INSERT IGNORE INTO movies (movie_id, title, genres) VALUES (?, ?, ?)";
        try (BufferedReader br = new BufferedReader(new FileReader(filePath));
             PreparedStatement pstmt = connection.prepareStatement(sql)) {
            br.readLine(); // Skip header line
            String line;
            while ((line = br.readLine()) != null) {
                String[] data = line.split(",", 3); // In case title has commas
                if (data.length >= 3) {
                    pstmt.setString(1, data[0]);
                    pstmt.setString(2, data[1]);
                    pstmt.setString(3, data[2]);
                    pstmt.executeUpdate();
                }
            }
            System.out.println("✅ Imported movies.csv");
        } catch (IOException | SQLException e) {
            System.out.println("⚠️ Error importing movies.csv: " + e.getMessage());
        }
    }

    private static void importRatings(Connection conn, String filePath) {
        String sql = "INSERT INTO ratings (rating_id, user_id, movie_id, rating) VALUES (?, ?, ?, ?)";
        try (BufferedReader br = new BufferedReader(new FileReader(filePath));
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            String line;
            boolean firstLine = true; // Skip header
            while ((line = br.readLine()) != null) {
                if (firstLine) {
                    firstLine = false;
                    continue;
                }

                String[] data = line.split(",");
                if (data.length >= 4) {
                    pstmt.setString(1, data[0]);
                    pstmt.setString(2, data[1]);
                    pstmt.setString(3, data[2]);
                    pstmt.setDouble(4, Double.parseDouble(data[3]));
                    pstmt.executeUpdate();
                }
            }
            System.out.println("✅ Imported ratings.csv");
        } catch (IOException | SQLException | NumberFormatException e) {
            System.out.println("⚠️ Error importing ratings.csv: " + e.getMessage());
        }
    }
}
