package com.sagebionetworks.bridge.migration.externalId;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

public class Utils {
    
    public static Connection establishConnection(String url, String username, String password, boolean useSsl) throws SQLException {
        String connectionString = getConnectionString(url, useSsl);
        return DriverManager.getConnection(connectionString, username, password);
    }
    
    public static String getConnectionString(String url, boolean useSsl) {
        // This fixes a timezone bug in the MySQL Connector/J
        url = url + "?serverTimezone=UTC";

        // Append SSL props to URL if needed
        if (useSsl) {
            url = url + "&requireSSL=true&useSSL=true&verifyServerCertificate=false";
        }
        // Connect to DB
        return url;
    }
    
    public static AmazonDynamoDBClient establishDynamoDBConnection(String key, String secretKey) {
        BasicAWSCredentials credentials = new BasicAWSCredentials(key, secretKey);
        return new AmazonDynamoDBClient(credentials);
    }
        
    public static void optionsToAccountUpdateSQL(List<String> statements, String id, JsonNode node) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE Accounts SET ");
        
        String extId = Utils.getString(node, "EXTERNAL_IDENTIFIER");
        if (extId != null) {
            sb.append("externalId = '"+extId+"', ");
        }
        String scope = Utils.getString(node, "SHARING_SCOPE");
        if (scope != null) {
            sb.append("sharingScope = '"+scope+"', ");
        }
        String zone = Utils.getString(node, "TIME_ZONE");
        if (zone != null) {
            sb.append("timeZone = '"+zone+"', ");
        }
        boolean notifyByEmail = Utils.getBoolean(node, "EMAIL_NOTIFICATIONS", true); 
        sb.append("notifyByEmail = ");
        sb.append(notifyByEmail);
        sb.append(", ");
        sb.append("migrationVersion = 1 WHERE id = '");
        sb.append(id);
        sb.append("';");
        statements.add(sb.toString());
    }
    
    public static void optionsToLanguagesUpdateSQL(List<String> statements, String id, JsonNode node) {
        for (String language : Utils.getList(node, "LANGUAGES")) {
            statements.add("INSERT INTO AccountLanguages (accountId, language) VALUES ('"+id+"','"+language+"');");    
        }
    }
    
    public static void optionsToDataGroupsUpdateSQL(List<String> statements, String id, JsonNode node) {
        for (String group : Utils.getList(node, "DATA_GROUPS")) {
            statements.add("INSERT INTO AccountDataGroups (accountId, dataGroup) VALUES ('"+id+"','"+group+"');");    
        }
    }
    
    public static String getString(JsonNode node, String field) {
        JsonNode child = node.get(field);
        if (child != null && !child.isNull() && !StringUtils.isBlank(child.textValue())) {
            return child.textValue();
        }
        return null;
    }

    public static boolean getBoolean(JsonNode node, String field, boolean defaultValue) {
        JsonNode child = node.get(field);
        if (child != null && child.isBoolean()) {
            return child.booleanValue();
        }
        if (child == null || child.isNull() || StringUtils.isBlank(child.textValue())) {
            return defaultValue;
        }
        return "true".equals(child.textValue());
    }
    
    public static List<String> getList(JsonNode node, String field) {
        JsonNode child = node.get(field);
        if (child != null && !child.isNull()) {
            String[] groups = child.textValue().split("\\s*,\\s*");
            List<String> list = Lists.newArrayList();
            for (String group : groups) {
                if (StringUtils.isNotBlank(group)) {
                    list.add(group);
                }
            }
            return list;
        }
        return new ArrayList<>();
    }
}
