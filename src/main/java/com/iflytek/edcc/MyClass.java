package com.iflytek.edcc;

import java.io.*;

class MyClass extends NewClass1 implements Serializable{

    private String s;
    private NewClass1 n;

    MyClass(String s) {
        this.s = s;
        setVal(20);
    }

    public String toString() {
        return s + " " + getVal();
    }

    public static void main(String args[]) {
        MyClass m = new MyClass("Serial");
        NewClass1 n = new NewClass1();
        n.setVal(10);
        try {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("serial.txt"));
//            oos.writeObject(n); //writing current state
            oos.writeObject(m); //writing current state
            oos.flush();
            oos.close();
            System.out.print(m); // display current state object value
        } catch (IOException e) {
            System.out.print(e);
        }
        try {
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream("serial.txt"));
            MyClass o = (MyClass) ois.readObject(); // reading saved object
            ois.close();
            System.out.print(o); // display saved object state
        } catch (Exception e) {
            System.out.print(e);
        }
    }
}
