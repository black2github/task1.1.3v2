package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class UserDaoJDBCImpl implements UserDao {
    private Connection connection = null;

    public UserDaoJDBCImpl() {
        try {
            connection = Util.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createUsersTable() {
        try (Statement statement = connection.createStatement()) {
            String sql = "CREATE TABLE IF NOT EXISTS Users ("
                    + "userId INT NOT NULL AUTO_INCREMENT,"
                    + "name VARCHAR(64) NULL,"
                    + "lastName VARCHAR(64) NULL,"
                    + "age INT NULL,"
                    + "PRIMARY KEY (`userId`));";
            statement.executeUpdate(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void dropUsersTable() {
        try (Statement statement = connection.createStatement()) {
            String sql = "DROP TABLE IF EXISTS Users;";
            statement.executeUpdate(sql);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        String sql = "INSERT into Users (name, lastName, age) "
                + " values (?,?,?) ";
        try (PreparedStatement pstm = connection.prepareStatement(sql)) {

            // Set values for parameters
            pstm.setString(1, name);
            pstm.setString(2, lastName);
            pstm.setInt(3, age);

            pstm.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void removeUserById(long id) {
        String sql = "DELETE from Users WHERE userId=" + id + ";";
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<User> getAllUsers() {
        List<User> list = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {

            String sql = "SELECT userId, name, lastName, age from Users;";

            // Execute SQL statement returns a ResultSet object.
            ResultSet rs = statement.executeQuery(sql);

            // Fetch on the ResultSet
            // Move the cursor to the next record.
            while (rs.next()) {
                long id = rs.getLong(1);
                String name = rs.getString(2);
                String lastName = rs.getString(3);
                byte age = (byte) rs.getInt(4);

                User user = new User(name, lastName, age);
                user.setId(id);
                list.add(user);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    public void cleanUsersTable() {
        String sql = "TRUNCATE Users;";
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
