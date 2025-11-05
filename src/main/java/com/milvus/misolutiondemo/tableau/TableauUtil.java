package com.milvus.misolutiondemo.tableau;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

public class TableauUtil {
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    public static String generateRandom(int length) {
        if (length <= 0) {
            throw new IllegalArgumentException("length > 0");
        }
        SecureRandom random = new SecureRandom();
        return IntStream.range(0, length).map(i -> {
            int randomIndex = random.nextInt(CHARACTERS.length());
            return CHARACTERS.charAt(randomIndex);
        }).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
    }

    public static Comparator<String> createNameComparator() {
        return (a, b) -> {
            int numA = extractNumber(a);
            int numB = extractNumber(b);

            if (numA == -1 && numB == -1) return a.compareTo(b);

            if (numA == -1) return -1;
            if (numB == -1) return 1;

            return Integer.compare(numB, numA);
        };
    }

    private static int extractNumber(String s) {
        String num = s.replaceAll("\\D+", ""); // Lấy phần số
        return num.isEmpty() ? -1 : Integer.parseInt(num);
    }

    public static void main(String[] args)  {
        demoNameComparator();
    }

    private static void demoNameComparator() {
        List<String> list = Arrays.asList("test", "test1", "test2", "test3");
        List<String> list1 = Arrays.asList("test1", "test11", "test12", "test13");
        list.sort(createNameComparator());
        list1.sort(createNameComparator());
        System.out.println(list);
        System.out.println(list1);
    }
}
