import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Main {

    private static BufferedReader reader;

    private static Connection connection;
    private static String query;
    private static PreparedStatement statement;

    private static final String CONNECTION_STRING = "jdbc:mysql://localhost:3306/";
    private static final String DATABASE_NAME = "minions_db";

    public static void main(String[] args) throws SQLException, IOException {

        reader = new BufferedReader(new InputStreamReader(System.in));

        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "");

        connection = DriverManager
                .getConnection(CONNECTION_STRING + DATABASE_NAME, properties);

        //2. Get Villains’ Names
        //getVillainsNamesAndCountMinions();

        // 3. Get Minion Names
        //getMinionsNamesExercise();

        // 4. Add Minion

        //addMinionInTheDatabaseExercise();

        // 5. Change Town Names Casing

        //changeTownNamesCasingExercise();

        // 7. Print All Minion Names

        //printAllMinionNamesExercise();

        // 8. Increase Minions Age

        //increaseMinionsAgeExercise();

        // 9. Increase Age Stored Procedure

        increaseAgeWithStoredProcedure();

    }

    private static void printAllMinionNamesExercise() throws SQLException {
        int numberOfMinions = getNumberOfMinions();
        if (numberOfMinions % 2 == 0) {
            int numberOfIterations = (numberOfMinions / 2);
            printMinions(numberOfIterations, numberOfMinions);
        }else {
            int numberOfIterations = (numberOfMinions / 2);
            System.out.println(numberOfIterations);
            printMinions(numberOfIterations, numberOfMinions);
            //printLastMinion(numberOfIterations);
        }
    }

    private static void printLastMinion(int numberOfIterations) throws SQLException {
        query = "SELECT name FROM minions WHERE id = ?";
        statement = connection.prepareStatement(query);
        statement.setInt(1, numberOfIterations + 1);
        ResultSet resultSet = statement.executeQuery();
        resultSet.next();
        System.out.printf("%s%n", resultSet.getString("name"));
    }

    private static void printMinions(int numberOfIterations, int numberOfMinions) throws SQLException {
        for (int i = 0; i < numberOfIterations; i++ ) {
            query = "SELECT name FROM minions WHERE id = ?";
            statement = connection.prepareStatement(query);
            statement.setInt(1, i + 1);
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            System.out.printf("%s%n", resultSet.getString("name"));

            query = "SELECT name FROM minions WHERE id = ?";
            statement = connection.prepareStatement(query);
            statement.setInt(1, numberOfMinions - i);
            resultSet = statement.executeQuery();
            resultSet.next();
            System.out.printf("%s%n", resultSet.getString("name"));
        }
    }

    private static int getNumberOfMinions() throws SQLException {
        query = "SELECT COUNT(id) AS c FROM minions";
        statement = connection.prepareStatement(query);
        ResultSet resultSet = statement.executeQuery();
        resultSet.next();
        return resultSet.getInt("c");
    }

    private static void increaseMinionsAgeExercise() throws IOException, SQLException {
        System.out.println("Insert minions id separated by space: ");
        String[] input = reader.readLine().split("\\s+");
        for (int i = 0; i < input.length; i++) {
            int min_id = Integer.parseInt(input[i]);
            String min_name = getEntityNameById(min_id, "minions");

            query = "UPDATE minions SET age = age + 1, name = LOWER(?) WHERE id = ?";
            statement = connection.prepareStatement(query);
            statement.setString(1, min_name);
            statement.setInt(2, min_id);

            statement.executeUpdate();
        }

        query = "SELECT name, age FROM minions";
        statement = connection.prepareStatement(query);
        ResultSet resultSet = statement.executeQuery();

        while (resultSet.next()) {
            System.out.printf("%s %d%n", resultSet.getString("name"), resultSet.getInt("age"));
        }

    }

    private static void increaseAgeWithStoredProcedure() throws IOException, SQLException {
        System.out.println("Enter minion id: ");
        int minion_id = Integer.parseInt(reader.readLine());

        query = "CALL usp_get_older(?)";
        CallableStatement callableStatement = connection.prepareCall(query);
        callableStatement.setInt(1, minion_id);
        callableStatement.execute();

        query = "SELECT * FROM minions WHERE id = ?";
        statement = connection.prepareStatement(query);
        statement.setInt(1, minion_id);
        ResultSet resultSet = statement.executeQuery();
        resultSet.next();
        System.out.printf("%s %d", resultSet.getString("name"), resultSet.getInt("age"));
    }

    private static void changeTownNamesCasingExercise() throws IOException, SQLException {
        System.out.println("Country name: ");
        String countryName = reader.readLine();

        if(!checkIfCountryExists(countryName)){
            System.out.println("No town names were affected.");
            return;
        }
        changeTownNamesToUpperCase(countryName);
        printMessage(countryName);

    }

    private static boolean checkIfCountryExists(String countryName) throws SQLException {
        query = "SELECT name FROM towns WHERE country = ?";
        statement = connection.prepareStatement(query);
        statement.setString(1, countryName);
        ResultSet resultSet = statement.executeQuery();

        return resultSet.next();
    }

    private static void printMessage(String cName) throws SQLException {
        query = "SELECT * FROM towns WHERE country = ?";
        statement = connection.prepareStatement(query);
        statement.setString(1, cName);
        ResultSet resultSet = statement.executeQuery();

        List<String> res = new ArrayList<>();
        while (resultSet.next()) {
            res.add(resultSet.getString("name"));
        }
        int num = res.size();
        System.out.printf("%d town names were affected.%n", num);
        System.out.println(res);
    }

    private static void changeTownNamesToUpperCase(String countryName) throws SQLException {
        query = "UPDATE towns\n" +
                "SET name = UPPER(name)\n" +
                "WHERE country = ?";
        statement = connection.prepareStatement(query);
        statement.setString(1, countryName);
        statement.executeUpdate();
    }

    private static void addMinionInTheDatabaseExercise() throws IOException, SQLException {

        System.out.println("Enter minion info:");
        String[] inputParams = reader.readLine().split("\\s+");
        String minionName = inputParams[0];
        int minionAge = Integer.valueOf(inputParams[1]);
        String minionTown = inputParams[2];

        System.out.println("Enter villain name:");
        String villainName = reader.readLine();

        if(!checkIfEntityExistsByName(minionTown, "towns")){
            insertEntityInTown(minionTown);

        }
        if(!checkIfEntityExistsByName(villainName, "villains")){
            insertEntityInVillains(villainName);
        }
        insertEntityInMinions(minionName, minionAge, minionTown, villainName);
    }

    private static void insertEntityInMinions(String minionName, int minionAge, String minionTown, String villainName) throws SQLException {

        int townId = getEntityIdByName(minionTown, "towns");
        query = "INSERT INTO minions(name, age, town_id) VALUE(?, ?, ?)";
        statement = connection.prepareStatement(query);
        statement.setString(1, minionName);
        statement.setInt(2, minionAge);
        statement.setInt(3, townId);

        statement.execute();

        insertEntitiesIntoMappingTable(minionName, villainName);

        System.out.printf("Successfully added %s to be minion of %s%n", minionName, villainName);
    }

    private static void insertEntitiesIntoMappingTable(String minionName, String villainName) throws SQLException {

        int minion_id = getEntityIdByName(minionName, "minions");
        int town_id = getEntityIdByName(villainName, "villains");

        query = "INSERT INTO minions_villains(minion_id, villain_id) VALUE(?, ?)";
        statement = connection.prepareStatement(query);
        statement.setInt(1, minion_id);
        statement.setInt(2, town_id);

        statement.execute();
    }

    private static int getEntityIdByName(String entityName, String tableName) throws SQLException {
        query = "SELECT id FROM " + tableName + " WHERE name = ?";
        statement = connection.prepareStatement(query);
        statement.setString(1, entityName);
        ResultSet resultSet = statement.executeQuery();
        resultSet.next();
        int res = resultSet.getInt("id");

        return res;
    }

    private static void insertEntityInVillains(String villainName) throws SQLException {
        query = "INSERT INTO villains (name, evilness_factor) VALUE (?, ?)";
        statement = connection.prepareStatement(query);
        statement.setString(1, villainName);
        statement.setString(2, "evil");

        statement.execute();

        System.out.printf("Villain %s was added to the database.%n", villainName);
    }

    private static void insertEntityInTown(String minionTown) throws SQLException {

        query = "INSERT INTO towns (name, country) VALUE (?, ?)";
        statement = connection.prepareStatement(query);
        statement.setString(1, minionTown);
        statement.setString(2, "NULL");

        statement.execute();
        System.out.printf("Town %s was added to the database.%n", minionTown);
    }

    private static boolean checkIfEntityExistsByName(String entityName, String tableName) throws SQLException {

        query = "SELECT * FROM " + tableName + " WHERE name = ?";
        statement = connection.prepareStatement(query);
        statement.setString(1, entityName);
        ResultSet resultSet = statement.executeQuery();

        return resultSet.next();
    }

    private static void getMinionsNamesExercise() throws IOException, SQLException {

        System.out.println("Enter villain id:");
        int villainId = Integer.parseInt(reader.readLine());

        if(!checkIfEntityExists(villainId, "villains")) {
            System.out.printf("No villain with ID %d exists in the database.", villainId);
            return;
        }

        System.out.printf("Villain: %s%n", getEntityNameById(villainId, "villains"));

        getMinionsNameAndAgeByVillainId(villainId);
    }

    private static void getMinionsNameAndAgeByVillainId(int villainId) throws SQLException {

        query = "SELECT m.name, m.age FROM minions AS m\n" +
                "JOIN minions_villains mv on m.id = mv.minion_id\n" +
                "WHERE mv.villain_id = ?";
        statement = connection.prepareStatement(query);
        statement.setInt(1, villainId);

        ResultSet resultSet = statement.executeQuery();
        int num = 0;
        while (resultSet.next()) {
            System.out.printf("%d. %s %d%n"
                    , ++num
                    , resultSet.getString("name")
                    , resultSet.getInt("age"));
        }
    }

    private static String getEntityNameById(int entityId, String tableName) throws SQLException {

        query = "SELECT name FROM " + tableName + " WHERE id = ?";
        statement = connection.prepareStatement(query);
        statement.setInt(1, entityId);
        ResultSet resultSet = statement.executeQuery();

        return resultSet.next() ? resultSet.getString("name"): null;
    }

    private static boolean checkIfEntityExists(int villainId, String villains) throws SQLException {
        query = "SELECT * FROM " + villains + " WHERE id = ?";

        statement = connection.prepareStatement(query);
        statement.setInt(1, villainId);
        ResultSet resultSet = statement.executeQuery();

        return resultSet.next();
    }

    private static void getVillainsNamesAndCountMinions() throws SQLException {

        query = "SELECT v.name, count(mv.minion_id) AS count_minions FROM villains AS v\n" +
                "JOIN minions_villains AS mv on v.id = mv.villain_id\n" +
                "GROUP BY v.name\n" +
                "HAVING count_minions > 15\n" +
                "ORDER BY count_minions DESC";
        statement = connection.prepareStatement(query);

        ResultSet resultSet = statement.executeQuery();

        while (resultSet.next()) {
            System.out.printf("%s %d%n", resultSet.getString(1)
                    , resultSet.getInt(2));
        }
    }
}
