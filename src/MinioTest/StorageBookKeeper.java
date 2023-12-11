package MinioTest;

import java.util.Random;

/**
 * I want to test the MinIO storage, I'll do a series of uploads/downloads/deletes. At the end how do I know the remote
 * storage has saved everything I want it to save and cleaned everything I don't want it to save?
 * I need to keep my own records about what files have been uploaded and what have been deleted.
 * This class is such a mirror that doesn't store actual objects but keeps a record
 */
public class StorageBookKeeper {

    public Random ran;
    public final int capacity;
    public Bucket[] buckets;
    public int bigObjectTotal = 0;
    public int smallObjectTotal = 0;
    public int bucketCount = 0;

    public StorageBookKeeper(Random _randomGenerator, int _capacity)
    {
        ran = _randomGenerator;
        capacity = _capacity;
        buckets = new Bucket[capacity];
    }


    static class ObjectInfo {
        public String name;
        public boolean isBigOrSmall;

        public ObjectInfo(String _name, boolean _isBig)
        {
            name = _name;
            isBigOrSmall = _isBig;
        }
    }

    class Bucket {
        public String name;
        public int objectCount = 0;
        public int bigObjectTotal = 0;
        public final int capacity;
        public ObjectInfo[] objects;

        private final String bigFilePath = "C:\\Users\\YuekunLi\\Downloads\\aws-java-sdk-1.12.350.zip";
        private final String smallFilePath = "C:\\Users\\YuekunLi\\text_file_2.txt";

        public Bucket(String _name, int _capacity)
        {
            name = _name;
            capacity = _capacity;
            objects = new ObjectInfo[capacity];
        }

        /**
         * get a spot that has object, if the capacity is 20, k controls the number of iteration, k goes from 1 to 20.
         * 'i' is the starting point, because I want randomness, if I always start at index 0, call this a few times
         * consecutively, I eventually the objects sequentially. "index" is the real index into the array, because when
         * 'i' starts in the middle, it's going to go beyond index 19, so it needs to wrap around.
         */
        public int getRandomObjectIndex()
        {
            int i = ran.nextInt(capacity);  // starting point
            int k = 1;  // k controls the number of iterations, I want as many as the capacity
            int index = 0;  // real index into the array, starting point can be anywhere, it will likely wrap over
            for (k = 1; k <= capacity; k++) {
                index = i % capacity;
                if (objects[index]!= null) {
                    break;
                }
                else
                {
                    i++;
                    k++;
                }
            }

            if (k > capacity)
                return -1;

            return index;
        }

        public int getRandomEmptySpotIndex()
        {
            int i = ran.nextInt(capacity);
            int k = 1; int index = 0;
            for (k = 1; k <= capacity; k++) {
                index = i % capacity;
                if (objects[index] == null) {
                    break;
                }
                else
                {
                    i++;
                    k++;
                }
            }

            if (k > capacity)
                return -1;

            return index;
        }

        public String getObjectName(int index)
        {
            return objects[index].name;
        }

        public boolean canUploadObject()
        {
            return objectCount < capacity;
        }

        public void uploadObject(String name, int index, boolean bigOrSmall)
        {
            objects[index] = new ObjectInfo(name, bigOrSmall);
            if (bigOrSmall)
            {
                bigObjectTotal++;
            }
        }
        public void deleteObject(int index)
        {
            if (objects[index].isBigOrSmall)
                bigObjectTotal--;

            objects[index] = null;
        }
    }

    public int getRandomNonEmptyBucketIndex()
    {
        int i = ran.nextInt(capacity);
        int k = 1; int index = 0;
        for (k = 1; k <= capacity; k++) {
            index = i % capacity;
            if (buckets[index] != null && buckets[index].objectCount > 0) {
                break;
            }
            else
            {
                i++;
                k++;
            }
        }

        if (k > capacity)
            return -1;

        return index;
    }


    public int getRandomEmptyBucketIndex()
    {
        int i = ran.nextInt(capacity);
        int k = 1; int index = 0;
        for (k = 1; k <= capacity; k++) {
            index = i % capacity;
            if (buckets[index]!= null && buckets[index].objectCount==0) {
                break;
            }
            else
            {
                i++;
                k++;
            }
        }

        if (k > capacity)
            return -1;

        return index;
    }


    /**
     * The bucket returned can be empty, but it must be one that has been created and used to have some objects,
     * the objects may have been deleted, but the bucket is still there.
     */
    public int getRandomBucketIndex()
    {
        int i = ran.nextInt(capacity);
        int k = 1; int index = 0;
        for (k = 1; k <= capacity; k++) {
            index = i % capacity;
            if (buckets[index] != null) {
                break;
            }
            else
            {
                i++;
                k++;
            }
        }

        if (k > capacity)
            return -1;

        return index;
    }

    public int getRandomAvailableSpotIndex()
    {
        int i = ran.nextInt(capacity);
        int k = 1; int index = 0;
        for (k = 1; k <= capacity; k++) {
            index = i % capacity;
            if (buckets[index] != null) {
                i++;
                k++;
            }
            else
                break;
        }

        if (k > capacity)
            return -1;

        return index;
    }

    public void createBucket(String bucketName, int index)
    {
        buckets[index] = new Bucket(bucketName, 20);
        bucketCount++;
    }

    public void deleteBucket(int index)
    {
        buckets[index] = null;
        bucketCount--;
    }

    public boolean canCreateBucket()
    {
        return bucketCount < capacity;
    }

    public String getBucketName(int index)
    {
        return buckets[index].name;
    }

    public boolean canUploadObject(int bucketIndex)
    {
        return buckets[bucketIndex].canUploadObject();
    }

    public void uploadObject(int bucketIndex, String objectName, int objectIndex, boolean isBig)
    {
        buckets[bucketIndex].uploadObject(objectName, objectIndex, isBig);
    }

    public int getEmptySpotInBucket(int bucketIndex)
    {
        return buckets[bucketIndex].getRandomEmptySpotIndex();
    }

    public int getObjectCountsInBucket(int bIndex)
    {
        return buckets[bIndex].objectCount;
    }
}
