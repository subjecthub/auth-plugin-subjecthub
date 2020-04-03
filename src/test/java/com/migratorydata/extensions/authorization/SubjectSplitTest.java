package com.migratorydata.extensions.authorization;

import org.junit.Test;

import java.util.Arrays;

import static com.migratorydata.extensions.authorization.DefaultAuthorizationListener.getSubjectHubIdAndGroup;
import static com.migratorydata.extensions.authorization.Groups.getSubjectSuffix;

public class SubjectSplitTest {

    @Test
    public void testSplit() {
        String subject1 = "/a/b/c";

        String[] elements = subject1.split("/");
        System.out.println(Arrays.toString(elements));
    }

    @Test
    public void testSubjectSuffix() {
        String subject = "/aa/bb/cc";
        String suffix = getSubjectSuffix(subject);
        System.out.println(suffix);
    }
}
