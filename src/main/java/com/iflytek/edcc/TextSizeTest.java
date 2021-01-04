package com.iflytek.edcc;

import java.io.UnsupportedEncodingException;

public class TextSizeTest {
    public static void main(String[] args) {
        try {
            System.out.println("旭日东升12acb".getBytes().length);
            System.out.println("旭日东升12acb".getBytes("GB2312").length);
            System.out.println("旭日东升12acb".getBytes("UTF-8").length);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
