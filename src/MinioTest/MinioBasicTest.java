package MinioTest;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.*;
import java.util.List;

import static java.lang.Thread.sleep;

public class MinioBasicTest {
    private final String region = Regions.US_WEST_1.name();
    private AmazonS3 s3Client;

    private final String downloadBucketPolicySingleLine = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"*\"]},\"Action\":[\"s3:GetBucketLocation\"],\"Resource\":[\"arn:aws:s3:::test-bucket-1\"]},{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"*\"]},\"Action\":[\"s3:ListBucket\"],\"Resource\":[\"arn:aws:s3:::test-bucket-1\"]},{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"*\"]},\"Action\":[\"s3:GetObject\"],\"Resource\":[\"arn:aws:s3:::test-bucket-1/test-object-1\"]}]}";


    private boolean setup () {
        try {
            String rootUserKeyId = "minio-admin-user";
            String rootUserSecretKey = "miniodeploy";
            AWSCredentials credentials = new BasicAWSCredentials(rootUserKeyId, rootUserSecretKey);
            ClientConfiguration clientConfiguration = new ClientConfiguration();
            clientConfiguration.setSignerOverride("AWSS3V4SignerType");

            String minioServerIp = "http://127.0.0.1:9000";
            s3Client = AmazonS3ClientBuilder.standard()
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(minioServerIp, region))
                    .withPathStyleAccessEnabled(true)
                    .withClientConfiguration(clientConfiguration)
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .build();
        } catch (Exception e) {
            System.out.println("Exception when building S3 Client");
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private boolean testSetPolicyForDownload() {
        String bucketName = "test-bucket-1";
        String objectName = "test-object-1";
        String downloadBucketPolicyMultiLine = " {                                                                     " +
                "   \"Version\":\"2012-10-17\",                                         " +
                "   \"Statement\":                                                      " +
                "   [                                                                   " +
                "     {                                                                 " +
                "       \"Effect\":\"Allow\",                                           " +
                "       \"Principal\":                                                  " +
                "       {                                                               " +
                "         \"AWS\":[\"*\"]                                               " +
                "       },                                                              " +
                "       \"Action\":[\"s3:GetBucketLocation\"],                          " +
                "       \"Resource\":[\"arn:aws:s3:::test-bucket-1\"]                   " +
                "     },                                                                " +
                "     {                                                                 " +
                "       \"Effect\":\"Allow\",                                           " +
                "       \"Principal\":{\"AWS\":[\"*\"]},                                " +
                "       \"Action\":[\"s3:ListBucket\"],                                 " +
                "       \"Resource\":[\"arn:aws:s3:::test-bucket-1\"]                   " +
                "     },                                                                " +
                "     {                                                                 " +
                "       \"Effect\":\"Allow\",                                           " +
                "       \"Principal\":{\"AWS\":[\"*\"]},                                " +
                "       \"Action\":[\"s3:GetObject\"],                                  " +
                "       \"Resource\":[\"arn:aws:s3:::test-bucket-1/test-object-1\"]     " +
                "     }                                                                 " +
                "   ]                                                                   " +
                " }                                                                     ";

        s3Client.createBucket(bucketName);
        File file = new File("C:\\Users\\YuekunLi\\text_file_2.txt");
        s3Client.putObject(new PutObjectRequest(bucketName, objectName, file));
        try {
            s3Client.setBucketPolicy(bucketName, downloadBucketPolicyMultiLine);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private boolean testBasics() {
        String bucketName = "test-bucket-2";
        String objectName = "test-object-2";

        try {
            System.out.println("Create a bucket:");
            s3Client.createBucket(bucketName);
            System.out.println();

            System.out.println("List buckets:");
            for (Bucket bucket : s3Client.listBuckets()) {
                System.out.println("    " + bucket.getName());
            }
            System.out.println();

            System.out.println("Upload an object:");
            File file = new File("C:\\Users\\YuekunLi\\text_file_1.txt");
            s3Client.putObject(new PutObjectRequest(bucketName, objectName, file));
            System.out.println();

            System.out.println("Download an object");
            S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, objectName));
            System.out.println("    Content-Type: " + object.getObjectMetadata().getContentType());
            displayTextInputStream(object.getObjectContent());
            System.out.println();

            System.out.println("List objects:");
            ObjectListing objectListing = s3Client.listObjects(new ListObjectsRequest()
                    .withBucketName(bucketName));
            for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
                System.out.println("    " + summary.getKey() + ",  size = " + summary.getSize());
            }
            System.out.println();

            System.out.println("Delete an object");
            s3Client.deleteObject(bucketName, objectName);
            System.out.println();

            System.out.println("Delete a bucket:");
            s3Client.deleteBucket(bucketName);
            System.out.println();

            List<Bucket> bucketsList = s3Client.listBuckets();
            int size = bucketsList.size();
            System.out.println("Number of buckets: " + Integer.toString(size));
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private void testTransferManager() {
        String bucketName = "test-bucket-3";
        String objectName = "test-object-3";
        try {
            TransferManager tx = TransferManagerBuilder.standard().withS3Client(s3Client).build();
            tx.getAmazonS3Client().createBucket(bucketName);
            File file = new File("C:\\Users\\YuekunLi\\Downloads\\aws-java-sdk-1.12.350.zip");
            PutObjectRequest req = new PutObjectRequest(bucketName, objectName, file);
            Upload upload = tx.upload(req);
            TransferProgress progress = upload.getProgress();
            double percent = progress.getPercentTransferred();
            while (percent < 100.0) {
                System.out.println("Percentage transfered: " + String.format("%.2f", percent));
                sleep(500);
                percent = progress.getPercentTransferred();
            }
            System.out.println("Percentage transfered: " + String.format("%.2f", percent));
            tx.shutdownNow();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void cleanup() {
        try {
            System.out.println("Clean up");
            for (Bucket bucket : s3Client.listBuckets()) {
                String bucketName = bucket.getName();
                ObjectListing objectListing = s3Client.listObjects(new ListObjectsRequest()
                        .withBucketName(bucketName));
                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                    String objectKey = objectSummary.getKey();
                    System.out.println("Delete object: " + bucketName + "/" + objectKey);
                    s3Client.deleteObject(bucketName, objectKey);
                }
                System.out.println("Delete bucket: " + bucketName);
                s3Client.deleteBucket(bucketName);
            }
        } catch (Exception e) {
            System.out.println("Exception when deleting old objects and buckets");
            e.printStackTrace();
        }
    }

    private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }

    public void run() {
        boolean ret = setup();
        if (!ret)
            return;


        //System.out.println("Basic Tests:");
        //testBasics();

        //System.out.println("Policy Tests:");
        //testSetPolicyForDownload();

        System.out.println("Test TransferManager:");
        testTransferManager();

        cleanup();
    }
}
