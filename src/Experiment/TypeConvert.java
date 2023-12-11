package Experiment;

import java.util.Arrays;

public class TypeConvert
{
    static void intToByteConvert()
    {
        byte [] a = new byte[] {-7, 7, -73, -65, 49, -117, 121, -3, 107, -99, -91, -119, 100, 111, -117, 29, -84, 119, -48, -56};
        System.out.println(Arrays.toString(a));

        StringBuilder s = new StringBuilder();

        for (byte b : a)
        {
            s.append(String.format("%8s", Integer.toBinaryString(((b+256)%256))).replace(' ', '0')).append("  ");
        }
        System.out.println(s);
    }

    static void printInVariousFormat()
    {
        byte[] a = new byte[]{0b00000001, 0b01111111, (byte)0b10000000, (byte)0b10000001, (byte)0b11111111};

        StringBuilder s = new StringBuilder();
        s.setLength(0);
        for (byte b : a)
        {
            s.append(String.format("%8s", Integer.toBinaryString(b)).replace(' ', '0')).append("  ");
                                     //  |<-------------------->|
                                     //    this first converts a byte to an integer
                                     // if the byte is a negative number, the resulted integer has a lot of ones from the left
                                     // for example if the byte is 0b10000011, the resulted integer is 0b11111.....11110011
                                     // even thought I want 8 digits by specifying %8s, that is not enforced.
        }
        System.out.println(s);

        s.setLength(0);
        for (byte b : a)
        {
            String binaryForm = String.format("%8s", Integer.toBinaryString(b)).replace(' ', '0');
            s.append(binaryForm.substring(binaryForm.length()-8)).append("  ");
        }
        System.out.println(s);

        s.setLength(0);
        for (byte b : a)
        {
            String binaryForm = String.format("%2s", Integer.toHexString(b)).replace(' ', '0');
            s.append(binaryForm.substring(binaryForm.length()-2)).append("  ");
        }
        System.out.println(s);
    }

    /**
     * The principles are:
     * (1) if I use binary or hex literal, those are the exact bits I want to store in memory
     * (2) if I use decimal, those are human-readable values, computer will store bits that are proper for those values
     * (3) when casting shorter data type to longer data type, the human-readable values are preserved, computer will store bits that are proper
     */
    static void byteInt()
    {
        byte[] a = new byte[]{0b00000001, 0b01111111, (byte)0b10000000, (byte)0b10000001, (byte)0b11111111};

        byte[] c = new byte[]{0x01,       0x7f,       (byte)0x80,       (byte)0x81,       (byte)0xff};

        byte[] d = new byte[]{1,          127,        -128,             -127,              -1};

        int[] e = new int[]{d[0], d[1], d[2], d[3], d[4]};

        int[] f = new int[]{d[0]+256, d[1]+256, d[2]+256, d[3]+256, d[4]+256};

        System.out.println(Arrays.toString(a));
        System.out.println(Arrays.toString(c));
        System.out.println(Arrays.toString(d));
        System.out.println(Arrays.toString(e));
        System.out.println(Arrays.toString(f));

        StringBuilder s = new StringBuilder();
        for (byte b : a)
        {
            s.append(String.format("%8s", Integer.toBinaryString(((b+256)%256))).replace(' ', '0')).append("  ");
        }
        System.out.println(s);

        s.setLength(0);
        for (byte b : c)
        {
            s.append(String.format("%8s", Integer.toBinaryString(((b+256)%256))).replace(' ', '0')).append("  ");
        }
        System.out.println(s);

        s.setLength(0);
        for (byte b : d)
        {
            s.append(String.format("%8s", Integer.toBinaryString(((b+256)%256))).replace(' ', '0')).append("  ");
        }
        System.out.println(s);

        s.setLength(0);
        for (int b : e)
        {
            s.append(String.format("%32s", Integer.toBinaryString(b)).replace(' ', '0')).append("  ");
        }
        System.out.println(s);

        int x = 0b11111111111111111111111110000001;
        System.out.println(x);
        System.out.println(Integer.toBinaryString(x));
    }
}
