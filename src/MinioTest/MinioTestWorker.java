package MinioTest;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

import java.util.Random;
import java.util.UUID;

public class MinioTestWorker implements Runnable {
    private final String keyId;
    private final String secretKey;
    private final String region = Regions.US_WEST_1.name();
    private AmazonS3 s3Client;
    private final StorageBookKeeper keeper;
    public Random ran;
    TransferManager tx;

    public enum BucketActions {
        CREATE,
        DELETE,
        LIST_OBJECTS
    }

    public enum ObjectActions {
        PUT,
        GET,
        XTRANSFER,
        DELETE,
        SET_POLICY
    }

    public MinioTestWorker(String _keyId, String _secretKey)
    {
        keyId = _keyId;
        secretKey = _secretKey;
        ran = new Random();
        keeper = new StorageBookKeeper(ran, 20);
    }
    private void setup () {
        try {
            AWSCredentials credentials = new BasicAWSCredentials(keyId, secretKey);
            ClientConfiguration clientConfiguration = new ClientConfiguration();
            clientConfiguration.setSignerOverride("AWSS3V4SignerType");

            String minioServerIp = "http://127.0.0.1:9000";
            s3Client = AmazonS3ClientBuilder.standard()
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(minioServerIp, region))
                    .withPathStyleAccessEnabled(true)
                    .withClientConfiguration(clientConfiguration)
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .build();

            tx = TransferManagerBuilder.standard().withS3Client(s3Client).build();
            //tx.getAmazonS3Client().createBucket(bucketName);

        } catch (Exception e) {
            System.out.println("Exception when building S3 Client");
            System.out.println(e.getMessage());
        }
    }
    private void createBucket()
    {
        if (keeper.canCreateBucket()) {
            int index = keeper.getRandomAvailableSpotIndex();
            if (index >= 0) {
                String name = UUID.randomUUID().toString();
                s3Client.createBucket(name);
                keeper.createBucket(name, index);
            }
        }
    }
    private void deleteBucket()
    {
        int index = keeper.getRandomEmptyBucketIndex();
        if (index >= 0) {
            String name = keeper.getBucketName(index);
            s3Client.deleteBucket(name);
            keeper.deleteBucket(index);
        }
    }

    private void listObjectsInBucket() {
        // what is the bucket count in the internal record
        int index = keeper.getRandomNonEmptyBucketIndex();
        int internalCount = keeper.getObjectCountsInBucket(index);
        String bName = keeper.getBucketName(index);

        ObjectListing objectListing = s3Client.listObjects(new ListObjectsRequest().withBucketName(bName));
        if (internalCount != objectListing.getObjectSummaries().size()) {
            throw new RuntimeException();
        }
    }

    private void pubObject(boolean isBig)
    {

    }

    private void getObject()
    {

    }

    private void transferObject()
    {

    }

    private void deleteObject()
    {

    }

    private void setPolicyOnObject()
    {

    }

    @Override
    public void run()
    {
        setup();

        int i = 0;
        while (i < 100) {  // randomly do 100 operations

            int bucketOrObject = ran.nextInt(2); // Am I doing an operation on a bucket or an object?

            if (bucketOrObject == 0) {
                BucketActions op = BucketActions.values()[ran.nextInt(3)];
                switch (op) {
                    case CREATE -> createBucket();
                    case DELETE -> deleteBucket();
                    case LIST_OBJECTS -> listObjectsInBucket();
                }
            } else {
                ObjectActions op = ObjectActions.values()[ran.nextInt(5)];
                switch (op) {
                    case PUT -> {
                        int bigOrSmall = ran.nextInt(2);
                        pubObject(bigOrSmall == 0);
                    }
                    case GET -> getObject();
                    case XTRANSFER -> transferObject();
                    case DELETE -> deleteObject();
                    case SET_POLICY -> setPolicyOnObject();
                }
            }
            i++;
        }

        // TODO:
        // verify the state of the remote storage matches the bookkeeper's record
    }
}
