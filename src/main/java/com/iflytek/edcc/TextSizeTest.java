package com.iflytek.edcc;

import java.io.UnsupportedEncodingException;

public class TextSizeTest {
    public static void main(String[] args) {
        try {
            System.out.println("旭日东升12acb".getBytes().length);
            System.out.println("旭日东升12acb".getBytes("GB2312").length);
            System.out.println("旭日东升12acb".getBytes("UTF-8").length);

            String s = "旭";
            char[] cs = s.toCharArray();
            byte[] bs = s.getBytes("GBK");
//            byte[] bs = s.getBytes();
//            for(char ci : cs){
//                System.out.println(ci);
//                System.out.println((int)ci);
//            }
            System.out.println("-------------------------");
            for(byte bi:bs){
                System.out.println(bi); //10
                System.out.println(Integer.toBinaryString(bi)); //2
                System.out.println(Integer.toHexString(bi)); //16
                System.out.println(Integer.toOctalString(bi)); //8
            }

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
