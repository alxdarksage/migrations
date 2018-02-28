package com.sagebionetworks.bridge.migration.externalId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class UtilsTest {

    // A bunch of real-life examples.
    private static final String[] TEST_JSON = new String[] {
        "{'EXTERNAL_IDENTIFIER':'BBB','SHARING_SCOPE':'NO_SHARING','EMAIL_NOTIFICATIONS':'false','LANGUAGES':'en','DATA_GROUPS':null}",
        "{'EXTERNAL_IDENTIFIER':null,'SHARING_SCOPE':'ALL_QUALIFIED_RESEARCHERS','EMAIL_NOTIFICATIONS':'true','LANGUAGES':'en','DATA_GROUPS':null}",
        "{'EXTERNAL_IDENTIFIER':null,'SHARING_SCOPE':'ALL_QUALIFIED_RESEARCHERS','EMAIL_NOTIFICATIONS':'true','LANGUAGES':null,'DATA_GROUPS':null}",
        "{'EXTERNAL_IDENTIFIER':null,'SHARING_SCOPE':'NO_SHARING','EMAIL_NOTIFICATIONS':'false','LANGUAGES':null,'DATA_GROUPS':null}",
        "{'EXTERNAL_IDENTIFIER':null,'SHARING_SCOPE':'NO_SHARING','EMAIL_NOTIFICATIONS':'true','LANGUAGES':'en','DATA_GROUPS':'group1,group2'}",
        "{'EXTERNAL_IDENTIFIER':null,'SHARING_SCOPE':'NO_SHARING','EMAIL_NOTIFICATIONS':'true','LANGUAGES':null,'DATA_GROUPS':'test_user'}",
        "{'EXTERNAL_IDENTIFIER':null,'SHARING_SCOPE':'SPONSORS_AND_PARTNERS','EMAIL_NOTIFICATIONS':'true','LANGUAGES':'en','DATA_GROUPS':null}",
        "{'EXTERNAL_IDENTIFIER':null,'SHARING_SCOPE':null,'EMAIL_NOTIFICATIONS':'true','LANGUAGES':'en','DATA_GROUPS':null}",
        "{'EXTERNAL_IDENTIFIER':null,'SHARING_SCOPE':null,'EMAIL_NOTIFICATIONS':'true','LANGUAGES':null,'DATA_GROUPS':'group1'}",
        "{'LANGUAGES':'en,fr'}",
        "{'SHARING_SCOPE':'ALL_QUALIFIED_RESEARCHERS','LANGUAGES':'en'}",
        "{}"
    };
    
    private static final String[] STATEMENTS = new String[] {
        "UPDATE Accounts SET externalId = 'BBB', sharingScope = 'NO_SHARING', notifyByEmail = false, migrationVersion = 1 WHERE id = 'ID';",
        "UPDATE Accounts SET sharingScope = 'ALL_QUALIFIED_RESEARCHERS', notifyByEmail = true, migrationVersion = 1 WHERE id = 'ID';",
        "UPDATE Accounts SET sharingScope = 'ALL_QUALIFIED_RESEARCHERS', notifyByEmail = true, migrationVersion = 1 WHERE id = 'ID';",
        "UPDATE Accounts SET sharingScope = 'NO_SHARING', notifyByEmail = false, migrationVersion = 1 WHERE id = 'ID';",
        "UPDATE Accounts SET sharingScope = 'NO_SHARING', notifyByEmail = true, migrationVersion = 1 WHERE id = 'ID';",
        "UPDATE Accounts SET sharingScope = 'NO_SHARING', notifyByEmail = true, migrationVersion = 1 WHERE id = 'ID';",
        "UPDATE Accounts SET sharingScope = 'SPONSORS_AND_PARTNERS', notifyByEmail = true, migrationVersion = 1 WHERE id = 'ID';",
        "UPDATE Accounts SET notifyByEmail = true, migrationVersion = 1 WHERE id = 'ID';",
        "UPDATE Accounts SET notifyByEmail = true, migrationVersion = 1 WHERE id = 'ID';",
        "UPDATE Accounts SET notifyByEmail = true, migrationVersion = 1 WHERE id = 'ID';",
        "UPDATE Accounts SET sharingScope = 'ALL_QUALIFIED_RESEARCHERS', notifyByEmail = true, migrationVersion = 1 WHERE id = 'ID';",
        "UPDATE Accounts SET notifyByEmail = true, migrationVersion = 1 WHERE id = 'ID';"
    };
    
    private static final String[] LANG_STATEMENTS = new String[] {
        "INSERT INTO AccountLanguages (accountId, language) VALUES ('ID','en');",
        "INSERT INTO AccountLanguages (accountId, language) VALUES ('ID','en');",
        null,
        null,
        "INSERT INTO AccountLanguages (accountId, language) VALUES ('ID','en');",
        null,
        "INSERT INTO AccountLanguages (accountId, language) VALUES ('ID','en');",
        "INSERT INTO AccountLanguages (accountId, language) VALUES ('ID','en');",
        null,
        "INSERT INTO AccountLanguages (accountId, language) VALUES ('ID','en');",
        "INSERT INTO AccountLanguages (accountId, language) VALUES ('ID','en');",
        null
    };
    
    private static final String[] GROUP_STATEMENTS = new String[] {
        null,
        null,
        null,
        null,
        "INSERT INTO AccountDataGroups (accountId, dataGroup) VALUES ('ID','group1');",
        "INSERT INTO AccountDataGroups (accountId, dataGroup) VALUES ('ID','test_user');",
        null,
        null,
        "INSERT INTO AccountDataGroups (accountId, dataGroup) VALUES ('ID','group1');",
        null,
        null,
        null
    };

    @Test
    public void getConnectionString() throws Exception {
        String value = Utils.getConnectionString("jdbc:mysql://localhost:3306/BridgeDB", true);
        assertEquals("jdbc:mysql://localhost:3306/BridgeDB?serverTimezone=UTC&requireSSL=true&useSSL=true&verifyServerCertificate=false", value);
    }
    
    @Test
    public void optionsToAccountUpdateSQLEmptyNode() throws Exception {
        List<String> statements = new ArrayList<>();
        Utils.optionsToAccountUpdateSQL(statements, "ID", JsonNodeFactory.instance.objectNode());
        
        assertEquals(1, statements.size());
        assertEquals("UPDATE Accounts SET notifyByEmail = true, migrationVersion = 1 WHERE id = 'ID';", statements.get(0));
    }

    @Test
    public void optionsToAccountUpdateSQL() throws Exception {
        for (int i=0; i < TEST_JSON.length; i++) {
            List<String> statements = new ArrayList<>();
            JsonNode node = createJson(TEST_JSON[i]);
            Utils.optionsToAccountUpdateSQL(statements, "ID", node);
            assertEquals(1, statements.size());
            assertEquals(STATEMENTS[i], statements.get(0));
            
        }
    }
    
    @Test
    public void optionsToLanguagesUpdateSQL() throws Exception {
        for (int i=0; i < TEST_JSON.length; i++) {
            List<String> statements = new ArrayList<>();
            JsonNode node = createJson(TEST_JSON[i]);
            Utils.optionsToLanguagesUpdateSQL(statements, "ID", node);
            if (statements.size() > 0) {
                assertEquals(LANG_STATEMENTS[i], statements.get(0));    
            } else if (LANG_STATEMENTS[i] != null) {
                fail("Should have produced a statement");
            }
        }
    }
    
    @Test
    public void languagesWillGenerateMultipleStatements() throws Exception {
        List<String> statements = new ArrayList<>();
        JsonNode node = createJson(TEST_JSON[9]);
        Utils.optionsToLanguagesUpdateSQL(statements, "ID", node);

        assertEquals("INSERT INTO AccountLanguages (accountId, language) VALUES ('ID','en');", statements.get(0));
        assertEquals("INSERT INTO AccountLanguages (accountId, language) VALUES ('ID','fr');", statements.get(1));
    }
    
    @Test
    public void nodeToDataGroupsUpdateSQL() throws Exception {
        for (int i=0; i < TEST_JSON.length; i++) {
            List<String> statements = new ArrayList<>();
            JsonNode node = createJson(TEST_JSON[i]);
            Utils.optionsToDataGroupsUpdateSQL(statements, "ID", node);
            if (statements.size() > 0) {
                assertEquals(GROUP_STATEMENTS[i], statements.get(0));    
            } else if (GROUP_STATEMENTS[i] != null) {
                fail("Should have produced a statement");
            }
        }
    }
    
    @Test
    public void dataGroupsWillGenerateMultipleStatements() throws Exception {
        List<String> statements = new ArrayList<>();
        JsonNode node = createJson(TEST_JSON[4]);
        Utils.optionsToDataGroupsUpdateSQL(statements, "ID", node);

        assertEquals("INSERT INTO AccountDataGroups (accountId, dataGroup) VALUES ('ID','group1');", statements.get(0));
        assertEquals("INSERT INTO AccountDataGroups (accountId, dataGroup) VALUES ('ID','group2');", statements.get(1));
    }
    
    @Test
    public void getString() throws Exception {
        String output = Utils.getString(createJson("{}"), "TEST");
        assertNull(output);
        
        output = Utils.getString(createJson("{'TEST':null}"), "TEST");
        assertNull(output);
        
        output = Utils.getString(createJson("{'TEST':''}"), "TEST");
        assertNull(output);
        
        output = Utils.getString(createJson("{'TEST':'VALUE'}"), "TEST");
        assertEquals("VALUE", output);
    }

    @Test
    public void getBoolean() throws Exception {
        boolean output = Utils.getBoolean(createJson("{}"), "TEST", false);
        assertFalse(output);
        
        output = Utils.getBoolean(createJson("{}"), "TEST", true);
        assertTrue(output);
        
        output = Utils.getBoolean(createJson("{'TEST':null}"), "TEST", false);
        assertFalse(output);
        
        output = Utils.getBoolean(createJson("{'TEST':null}"), "TEST", true);
        assertTrue(output);
        
        output = Utils.getBoolean(createJson("{'TEST':''}"), "TEST", false);
        assertFalse(output);
        
        output = Utils.getBoolean(createJson("{'TEST':''}"), "TEST", true);
        assertTrue(output);
        
        output = Utils.getBoolean(createJson("{'TEST':'true'}"), "TEST", false);
        assertTrue(output);
        
        output = Utils.getBoolean(createJson("{'TEST':'false'}"), "TEST", true);
        assertFalse(output);
        
        output = Utils.getBoolean(createJson("{'TEST':true}"), "TEST", false);
        assertTrue(output);
        
        output = Utils.getBoolean(createJson("{'TEST':false}"), "TEST", true);
        assertFalse(output);
    }
    
    @Test
    public void getList() throws Exception {
        List<String> output = Utils.getList(createJson("{}"), "TEST");
        assertTrue(output.isEmpty());
        
        output = Utils.getList(createJson("{}"), "TEST");
        assertTrue(output.isEmpty());
        
        output = Utils.getList(createJson("{'TEST':null}"), "TEST");
        assertTrue(output.isEmpty());
        
        output = Utils.getList(createJson("{'TEST':''}"), "TEST");
        assertTrue(output.isEmpty());
        
        output = Utils.getList(createJson("{'TEST':'group1'}"), "TEST");
        assertEquals(Lists.newArrayList("group1"), output);

        output = Utils.getList(createJson("{'TEST':'group1,group2'}"), "TEST");
        assertEquals(Lists.newArrayList("group1", "group2"), output);
    }
    
    private JsonNode createJson(String json) throws Exception {
        return new ObjectMapper().readTree(json.replaceAll("'", "\""));
    }
}
