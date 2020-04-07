package com.migratorydata.extensions.authorization;

import org.junit.Test;

import java.util.Arrays;

public class SubjectSplitTest {

    @Test
    public void testSplit() {
        String subject1 = "/a/b/c";

        String[] elements = subject1.split("/");
        System.out.println(Arrays.toString(elements));
    }
}
