package com.iflytek.edcc;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;
import scala.math.BigInt;

import java.io.*;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * 实现自定义聚合函数Bitmap
 */
public class UdafBitMap extends UserDefinedAggregateFunction {

    /**
     * int到byte[] 由高位到低位
     * @param i 需要转换为byte数组的整行值。
     * @return byte数组
     */
    public static byte[] intToByteArray(int i) {
        byte[] result = new byte[4];
        result[0] = (byte)((i >> 24) & 0xFF);
        result[1] = (byte)((i >> 16) & 0xFF);
        result[2] = (byte)((i >> 8) & 0xFF);
        result[3] = (byte)(i & 0xFF);
        return result;
    }

    /**
     * byte[]转int
     * @param bytes 需要转换成int的数组
     * @return int值
     */
    public static int byteArrayToInt(byte[] bytes) {
        int value=0;
        for(int i = 0; i < 4; i++) {
            int shift= (3-i) * 8;
            value +=(bytes[i] & 0xFF) << shift;
        }
        return value;
    }

    @Override
    public StructType inputSchema() {
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("field", DataTypes.IntegerType, true));
        return DataTypes.createStructType(structFields);
    }

    @Override
    public StructType bufferSchema() {
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("field", DataTypes.BinaryType, true));
        return DataTypes.createStructType(structFields);
    }

    @Override
    public DataType dataType() {
        return DataTypes.LongType;
    }

    @Override
    public boolean deterministic() {
        //是否强制每次执行的结果相同
        return false;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        //初始化
        buffer.update(0, null);
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        // 相同的executor间的数据合并
        // 1. 输入为空直接返回不更新
        Object in = input.get(0);
        if(in == null){
            return ;
        }
        // 2. 源为空则直接更新值为输入
        RoaringBitmap processRR = new RoaringBitmap();
        ByteArrayOutputStream processbos = new ByteArrayOutputStream();
        try {
            // 新增的值序列化成字节数组
            processRR.add((Integer)in);
            processRR.serialize(new DataOutputStream(processbos));
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] inBytes = processbos.toByteArray();

        Object out = buffer.get(0);
        if(out == null){
            System.out.println("======初始化 buffer========");
            System.out.println(in);
            System.out.println("======初始化 buffer========");
            buffer.update(0, inBytes);
            return ;
        }
        // 3. 源和输入都不为空使用bitmap去重合并
        byte[] outBytes = (byte[]) out;
        byte[] result = outBytes;
        RoaringBitmap outRR = new RoaringBitmap();
        RoaringBitmap inRR = new RoaringBitmap();
        try {
            // 反序列化，获取数据
            outRR.deserialize(new DataInputStream(new ByteArrayInputStream(outBytes)));
            inRR.deserialize(new DataInputStream(new ByteArrayInputStream(inBytes)));
            outRR.or(inRR);

            System.out.println("***********update处理*****************");
            System.out.println(in);
            System.out.println("update处理 buffer");
            System.out.println(outRR.getCardinality());
            System.out.println("************update处理****************");

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            outRR.serialize(new DataOutputStream(bos));
            result = bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        buffer.update(0, result);
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        //不同excutor间的数据合并
        // 相同的executor间的数据合并
        // 1. 输入为空直接返回不更新
        Object in = buffer2.get(0);
        if(in == null){
            return ;
        }
        // 2. 源为空则直接更新值为输入
        byte[] inBytes = (byte[]) in;
        Object out = buffer1.get(0);
        if(out == null){
            buffer1.update(0, inBytes);
            return ;
        }
        // 3. 源和输入都不为空使用bitmap去重合并
        byte[] outBytes = (byte[]) out;
        byte[] result = outBytes;
        RoaringBitmap outRR = new RoaringBitmap();
        RoaringBitmap inRR = new RoaringBitmap();
        try {
            outRR.deserialize(new DataInputStream(new ByteArrayInputStream(outBytes)));
            inRR.deserialize(new DataInputStream(new ByteArrayInputStream(inBytes)));
            outRR.or(inRR);

            System.out.println("***********merge处理*****************");
            System.out.println(in);
            System.out.println("merge处理 buffer");
            outRR.forEach((IntConsumer) i -> System.out.println(i));
            System.out.println("***********merge处理*****************");

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            outRR.serialize(new DataOutputStream(bos));
            result = bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        buffer1.update(0, result);
    }

    @Override
    public Object evaluate(Row buffer) {
        //根据Buffer计算结果
        long r = 0l;
        Object val = buffer.get(0);
        if (val != null) {
            RoaringBitmap rr = new RoaringBitmap();
            try {
                System.out.println(val);
                rr.deserialize(new DataInputStream(new ByteArrayInputStream((byte[])val)));
                System.out.println("=========getLongCardinality==========");
                System.out.println(rr.getLongCardinality());
                r = rr.getLongCardinality();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return r;
    }
}