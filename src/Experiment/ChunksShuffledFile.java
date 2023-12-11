package Experiment;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class ChunksShuffledFile
{
    static final byte leftbit = (byte)0b10000000;

    static int getVLQSize(byte[] a, int offset)
    {
        int size = 1;
        for (int i = offset; i < a.length && (a[i] & leftbit) == (byte)0b10000000; i++)
        {
            size++;
        }
        return size;
    }

    static long getVLQ(byte[] a, int offset)
    {
        long value = 0;
        for (int i = offset; ; i++)
        {
            if (i == a.length)
            {
                throw new IllegalArgumentException("last byte wrong");
            }
            if (i - offset == 10)
            {
                throw new IllegalArgumentException("too long");
            }
            value |= (a[i] & 0b01111111);
            if ((a[i] & leftbit) == 0)
            {
                break;
            }
            value = value << 7;
        }
        return value;
    }

    /**
     * If I use boolean as return type and use the return value to indicate match or not,
     * there is no way to gracefully tell the caller that the comparison didn't happen due to exception.
     * I have to let the exception bubble up.
     */
    static boolean isMatch(ByteBuffer b1, ByteBuffer b2) throws NoSuchAlgorithmException
    {
        byte[] a1 = b1.array();
        byte[] a2 = b2.array();
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        md.reset();
        md.update(a1);
        byte[] hash1 = md.digest();
        md.reset();
        md.update(a2);
        byte[] hash2 = md.digest();
        return Arrays.equals(hash1, hash2);
    }


    static void compareChunksShuffledFileWithRawFile(String csFilePath, String rawFilePath)
    {
        RandomAccessFile csFile;  // cs: chunks shuffled
        FileChannel csFileChannel;

        RandomAccessFile rawFile;
        FileChannel rawFileChannel;

        try
        {
            csFile = new RandomAccessFile(csFilePath, "r");
            rawFile = new RandomAccessFile(rawFilePath, "r");
        }
        catch (FileNotFoundException e)
        {
            System.out.println("Fail, instantiate random access file");
            e.printStackTrace();
            return;
        }

        csFileChannel = csFile.getChannel();
        rawFileChannel = rawFile.getChannel();

        ByteBuffer bb = ByteBuffer.allocate(15);
        byte[] ba = new byte[15];

        int blocksCount = 1;

        StringBuilder s = new StringBuilder();

        try
        {
            long fileSize = csFileChannel.size();
            long offsetInChunksShuffledFile = 0;
            csFileChannel.position(0);
            while (csFileChannel.position() < fileSize)
            {
                int br = csFileChannel.read(bb);
                bb.flip();
                bb.get(ba, 0, br);

                int sz1 = getVLQSize(ba, 0);
                long v1 = getVLQ(ba, 0);
                int sz2 = getVLQSize(ba, sz1);
                long v2 = getVLQ(ba, sz1);

                if (v2 > Integer.MAX_VALUE)
                    throw new RuntimeException("v2 overflow");

                offsetInChunksShuffledFile += (sz1 + sz2);

                if (fileSize - offsetInChunksShuffledFile < v2)
                {
                    throw new RuntimeException("rest of file not long enough");
                }

                ByteBuffer dataInChunksShuffledFile = ByteBuffer.allocate((int)v2);
                csFileChannel.read(dataInChunksShuffledFile, offsetInChunksShuffledFile);

                ByteBuffer dataInRawFile = ByteBuffer.allocate((int)v2);
                rawFileChannel.read(dataInRawFile, v1);

                try {
                    if (!isMatch(dataInChunksShuffledFile, dataInRawFile)) {
                        System.out.println("mismatch, offset: " + v1 + ", size: " + v2 + ", block start in chunks shuffled file: " + (offsetInChunksShuffledFile - sz1 - sz2) + ", sz1: " + sz1 + ", sz2: " + sz2);
                        s.setLength(0);
                        for (byte b : ba) {
                            String binaryForm = String.format("%8s", Integer.toBinaryString(b)).replace(' ', '0');
                            s.append(binaryForm.substring(binaryForm.length() - 8)).append("  ");
                        }
                        System.out.println("header: " + s);
                    }
                }
                catch (NoSuchAlgorithmException e)
                {
                    //System.out.println("No required hash algorithm");
                    e.printStackTrace();
                    break;
                }

                offsetInChunksShuffledFile += v2;
                csFileChannel.position(offsetInChunksShuffledFile);
                Arrays.fill(ba, (byte)0);
                bb.clear();
                blocksCount++;
            }
            System.out.println(blocksCount);
            csFile.close();
            rawFile.close();
        }
        catch (java.io.IOException e)
        {
            e.printStackTrace();
        }
    }

    static void seekBlockInChunksShuffledFile(String csFilePath, int offsetInRawFile)
    {
        //String path = "C:\\Users\\YuekunLi\\Downloads\\-93.sf";
        RandomAccessFile csFile;
        FileChannel csFileChannel;
        try
        {
            csFile = new RandomAccessFile(csFilePath, "r");
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
            return;
        }

        csFileChannel = csFile.getChannel();

        ByteBuffer bb = ByteBuffer.allocate(15);
        byte[] ba = new byte[15];

        int blocksCount = 1;

        StringBuilder s = new StringBuilder();

        try
        {
            long fileSize = csFileChannel.size();
            long offsetInChunksShuffledFile = 0;
            csFileChannel.position(0);
            while (csFileChannel.position() < fileSize)
            {
                int br = csFileChannel.read(bb);
                bb.flip();
                bb.get(ba, 0, br);

                int sz1 = getVLQSize(ba, 0);
                long v1 = getVLQ(ba, 0);
                int sz2 = getVLQSize(ba, sz1);
                long v2 = getVLQ(ba, sz1);

                if (v2 > Integer.MAX_VALUE)
                    throw new RuntimeException("v2 overflow");

                if (v1 == offsetInRawFile)
                {
                    System.out.println("Found, offset: " + v1 + ", size: " + v2 + ", block start in chunks shuffled file: " + offsetInChunksShuffledFile + ", sz1: " + sz1 + ", sz2: " + sz2);
                    s.setLength(0);
                    for (byte b : ba)
                    {
                        String binaryForm = String.format("%8s", Integer.toBinaryString(b)).replace(' ', '0');
                        s.append(binaryForm.substring(binaryForm.length()-8)).append("  ");
                    }
                    System.out.println("header: " + s);


                    ByteBuffer dataInChunksShuffledFile = ByteBuffer.allocate((int)v2);
                    byte[] dataArray = new byte[(int)v2];
                    csFileChannel.read(dataInChunksShuffledFile, offsetInChunksShuffledFile+sz1+sz2);
                    dataInChunksShuffledFile.flip();
                    dataInChunksShuffledFile.get(dataArray);

                    s.setLength(0);
                    for (byte b : dataArray)
                    {
                        String binaryForm = String.format("%2s", Integer.toHexString(b)).replace(' ', '0');
                        s.append(binaryForm.substring(binaryForm.length()-2)).append(" ");
                    }

                    System.out.println("Content: " + s);
                }

                offsetInChunksShuffledFile += ((sz1 + sz2) + v2);
                csFileChannel.position(offsetInChunksShuffledFile);
                Arrays.fill(ba, (byte)0);
                bb.clear();
                blocksCount++;
            }
            System.out.println(blocksCount);
            csFile.close();
        }
        catch (java.io.IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Should I use the Knuth-Morris-Pratt algorithm?
     * No, because that is rather slow in this special case.
     * The pattern is all zero.
     * pattern: 0, 0, 0, 0, 0, 0, 0, ......
     * For pattern at index 4, what is the prefix of the pattern that matches the end of the pattern[0-4] substring?
     * It's exactly pattern[0-3], because these elements are all the same.
     * Remember I can't say the prefix that matches the end of pattern[0-4] is still pattern[0-4],
     * otherwise I fall in dead loop.
     * The prefix for the end of pattern[0-3] is, surprisingly, pattern[0-2].
     * So I would repeatedly try pattern[0-4], pattern[0-3], ......, pattern[0], none of which can advance the matching sequence
     * because the next element in the searched array is not zero.
     * Therefore, the KMP algorithm in this special case is rather slow.
     */
    static boolean isConsecutiveZeros(byte[] a, int consecutiveCount)
    {
        for (int i = 0; i < a.length; i++)
        {
            if (a[i] == (byte)0)
            {
                int j = i;
                while (j < a.length)
                {
                    if (a[j] != (byte)0)
                    {
                        break;
                    }
                    j++;
                }
                if (j - i >= consecutiveCount)
                {
                    System.out.println("Consecutive zeros: " + i + ",  " + j + ", (" + (j-i) + ")");
                    return true;
                }
                else
                {
                    i = j;
                }
            }
        }
        return false;
    }

    static void checkConsecutiveZerosInChunksShuffledFile(String path)
    {
        RandomAccessFile file;
        FileChannel channel;

        try
        {
            file = new RandomAccessFile(path, "r");
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
            return;
        }
        channel = file.getChannel();

        ByteBuffer bb = ByteBuffer.allocate(15);
        byte[] ba = new byte[15];

        int blocksCount = 0;

        try
        {
            long fileSize = channel.size();
            long offsetInChunksShuffledFile = 0;
            channel.position(0);
            while (channel.position() < fileSize)
            {
                int br = channel.read(bb);
                bb.flip();
                bb.get(ba, 0, br);

                int sz1 = getVLQSize(ba, 0);
                long v1 = getVLQ(ba, 0);
                int sz2 = getVLQSize(ba, sz1);
                long v2 = getVLQ(ba, sz1);

                if (v2 > Integer.MAX_VALUE)
                    throw new RuntimeException("v2 overflow");

                offsetInChunksShuffledFile += (sz1 + sz2);

                if (fileSize - offsetInChunksShuffledFile < v2)
                {
                    throw new RuntimeException("rest of file not long enough");
                }

                ByteBuffer dataInChunksShuffledFile = ByteBuffer.allocate((int)v2);
                channel.read(dataInChunksShuffledFile, offsetInChunksShuffledFile);

                byte[] dataArray = dataInChunksShuffledFile.array();

                if (isConsecutiveZeros(dataArray, 1000))
                {
                    System.out.println("offset: " + v1 + ",  size: " + v2);
                }

                offsetInChunksShuffledFile += v2;
                channel.position(offsetInChunksShuffledFile);
                Arrays.fill(ba, (byte)0);
                bb.clear();
                blocksCount++;
            }
            System.out.println("Total number of blocks: " + blocksCount);
            file.close();
        }
        catch (java.io.IOException e)
        {
            e.printStackTrace();
        }
    }
}
