package aws.s3.lxzl.client;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Main {

	public static void main(String[] args) {

		final AmazonS3 s3 = AmazonS3Client.builder().withRegion(Regions.AP_NORTHEAST_1)
				.withCredentials(new ProfileCredentialsProvider()).build();

		List<Bucket> buckets = s3.listBuckets();
		System.out.println("Your Amazon S3 buckets are:");
		for (Bucket b : buckets) {
			System.out.println("* " + b.getName());
		}

		String bucket_name = "test-for-lsh-file-server";
		String file_path = "C:\\Users\\nannan.c.wang\\Downloads\\20171229173606486.png";
		String key_name = "uploadFile02";

		
		System.out.format("Uploading %s to S3 bucket %s...\n", file_path, bucket_name);
		try {
			s3.putObject(bucket_name, key_name, new File(file_path));
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}

		
		System.out.println(" - listing objects from bucket [" + bucket_name + "]");
		ListObjectsV2Result result = s3.listObjectsV2(bucket_name);
		List<S3ObjectSummary> objects = result.getObjectSummaries();
		for (S3ObjectSummary os: objects) {
		    System.out.println("* " + os.getKey());
		}
		
	}
}
