package com.migratorydata.extensions.authorization;

import org.json.JSONObject;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SubjectSplitTest {

    @Test
    public void testSplit() {
        String subject1 = "/a/b";

        String[] elements = subject1.substring(1).split("/");
        for (int i = 0; i < elements.length; i++) {
            System.out.print(elements[i]);
        }
        System.out.println(Arrays.toString(elements));
    }

    @Test
    public void testSplit_() {
        String subject1 = "a:b:c:d";

        String[] elements = subject1.split(":");

        for (int i = 0; i < elements.length; i++) {
            System.out.print(elements[i]);
        }

        System.out.println(Arrays.toString(elements));
    }

    @Test
    public void testMapToJSon() {
        Map<String, Integer> topicToConnections = new HashMap<>();
        topicToConnections.put("aaa", 1);
        topicToConnections.put("ccc", 3);

        JSONObject obj = new JSONObject(topicToConnections);

        System.out.println(obj.toString());
    }
}
