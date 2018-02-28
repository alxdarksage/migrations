package com.sagebionetworks.bridge.migration.externalId;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

public class ExternalIdApplication {

    private final static Logger LOGGER = Logger.getLogger(ExternalIdApplication.class.getName());

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final JsonNode EMPTY_NODE = JsonNodeFactory.instance.objectNode();

    private Connection connection;
    private AmazonDynamoDBClient client;
    private String ddbTable;
    private int limit;

    public static void main(String[] args) throws Exception {
        LOGGER.info("Loading config from file: " + args[0]);

        Properties properties = new Properties();
        properties.load(new FileInputStream(args[0]));

        String ddbTable = properties.getProperty("ddb.table");
        
        int limit = Integer.parseInt(properties.getProperty("limit"));

        String url = properties.getProperty("mysql.url");
        String username = properties.getProperty("mysql.username");
        String password = properties.getProperty("mysql.password");
        boolean useSsl = Boolean.parseBoolean(properties.getProperty("mysql.useSsl"));
        Connection connection = Utils.establishConnection(url, username, password, useSsl);

        String key = properties.getProperty("aws.key");
        String secretKey = properties.getProperty("aws.secret.key");
        AmazonDynamoDBClient client = Utils.establishDynamoDBConnection(key, secretKey);

        new ExternalIdApplication(ddbTable, connection, client, limit).run();
    }

    public ExternalIdApplication(String ddbTable, Connection connection, AmazonDynamoDBClient client, int limit) {
        this.ddbTable = ddbTable;
        this.connection = connection;
        this.client = client;
        this.limit = limit;
    }

    void run() throws Exception {
        Map<String, String> userIds = collectAccountsToMigrate();

        LOGGER.info("Found " + userIds.size() + " records (limit = "+limit+").");
        for (Map.Entry<String, String> entry : userIds.entrySet()) {
            migrateAccount(entry);
            Thread.sleep(200);
        }
        connection.close();
    }

    protected void migrateAccount(Map.Entry<String, String> entry) throws Exception, SQLException {
        LOGGER.info("Migrating user " + entry.getKey());

        String userId = entry.getKey();
        String healthCode = entry.getValue();
        JsonNode node = getDynamoDBRecord(healthCode);

        connection.setAutoCommit(false);
        try (Statement statement = connection.createStatement()) {
            List<String> statements = new ArrayList<>();
            Utils.optionsToAccountUpdateSQL(statements, userId, node);
            Utils.optionsToLanguagesUpdateSQL(statements, userId, node);
            Utils.optionsToDataGroupsUpdateSQL(statements, userId, node);

            for (String sql : statements) {
                statement.executeUpdate(sql);
            }
            connection.commit();
        } catch (SQLException e) {
            LOGGER.warning(e.getMessage());
            connection.rollback();
        }
    }

    protected Map<String, String> collectAccountsToMigrate() throws SQLException {
        Map<String, String> userIds = new HashMap<>();
        connection.setAutoCommit(true);
        try (Statement statement = connection.createStatement()) {
            ResultSet results = statement.executeQuery("SELECT id, healthCode, migrationVersion "
                    + "FROM Accounts WHERE migrationVersion != 1 LIMIT " + limit);
            while (results.next()) {
                String userId = results.getString("id");
                String healthCode = results.getString("healthCode");
                userIds.put(userId, healthCode);
            }
        }
        return userIds;
    }

    protected JsonNode getDynamoDBRecord(String healthCode) throws Exception {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("healthDataCode", new AttributeValue().withS(healthCode));

        GetItemRequest request = new GetItemRequest().withTableName(ddbTable).withKey(key).withAttributesToGet("data",
                "studyKey");

        GetItemResult result = client.getItem(request);

        if (result.getItem() != null) {
            String json = result.getItem().get("data").getS();
            return MAPPER.readTree(json);
        }
        return EMPTY_NODE;
    }
}
