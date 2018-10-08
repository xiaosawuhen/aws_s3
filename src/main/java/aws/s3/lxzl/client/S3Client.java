package aws.s3.lxzl.client;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3Client {

	public static void main(String[] args) {

		File file = new File("C:\\\\Users\\\\nannan.c.wang\\\\Downloads\\\\20171229173606486.png");

		S3Client client = new S3Client("cn-north-1").changeBucket("codis3.gileadchina.cn");
		
		String folderName = "lsh_java_test_wnn";

		client.createFolder(folderName);

		 PutObjectResult result = client.uploadFile(folderName + "/uploadFile_04.png", file);

//		List<S3ObjectSummary> objects = client.listFiles();
//		for (S3ObjectSummary os : objects) {
//			System.out.println("* " + os.getKey());
//		}

		// client.downloadFile("test.txt", "C:\\Users\\nannan.c.wang\\Desktop");
	}

	private String bucket_name;
	private String region;
	
//	private String accessKey="AKIAIZAWNXOBB54QKBAQ";
//	private String secretKey="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";
	private String accessKey="AKIAPRHWAKXNJLYVXFKQ";
	private String secretKey="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";
	private AmazonS3 s3;

	public S3Client(String region) {
		this.region = region;

		AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

//		s3 = AmazonS3Client.builder().withRegion(this.region).withCredentials(new ProfileCredentialsProvider()).build();
		s3 = AmazonS3Client.builder()
				.withRegion(this.region)
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.build();
	}

	public S3Client changeBucket(String bucket_name) {
		this.bucket_name = bucket_name;
		return this;
	}

	public PutObjectResult createFolder(String folderName) {

		PutObjectResult result = null;
		// create meta-data for your folder and set content-length to 0
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(0);
		// create empty content
		InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
		// create a PutObjectRequest passing the folder name suffixed by /
		PutObjectRequest putObjectRequest = new PutObjectRequest(bucket_name, folderName + "/", emptyContent, metadata);
		// send request to S3 to create folder
		try {
			result = s3.putObject(putObjectRequest);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
		}

		return result;
	}

	public PutObjectResult uploadFile(String key_name, File file) {
		System.out.format("Uploading %s to S3 bucket %s...\n", file.getAbsolutePath(), bucket_name);

		PutObjectResult result = null;
		try {
			s3.putObject(bucket_name, key_name, file);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
		}

		return result;
	}

	public List<S3ObjectSummary> listFiles() {
		System.out.println(" - listing objects from bucket [" + bucket_name + "]");
		ListObjectsV2Result result = s3.listObjectsV2(bucket_name);
		List<S3ObjectSummary> objects = result.getObjectSummaries();
		return objects;
	}

	public boolean downloadFile(String key_name, String file_path) {
		boolean result = true;
		System.out.format("Downloading %s from S3 bucket %s...\n", key_name, bucket_name);
		try {
			S3Object o = s3.getObject(bucket_name, key_name);
			S3ObjectInputStream s3is = o.getObjectContent();
			FileOutputStream fos = new FileOutputStream(new File(file_path + File.separator + key_name));
			byte[] read_buf = new byte[1024];
			int read_len = 0;
			while ((read_len = s3is.read(read_buf)) > 0) {
				fos.write(read_buf, 0, read_len);
			}
			s3is.close();
			fos.close();
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			result = false;
		} catch (FileNotFoundException e) {
			System.err.println(e.getMessage());
			result = false;
		} catch (IOException e) {
			System.err.println(e.getMessage());
			result = false;
		}
		return result;
	}

	public boolean deleteFile(String key_name) {
		boolean result = true;
		System.out.format("Deleting %s from S3 bucket %s...\n", key_name, bucket_name);
		try {
			s3.deleteObject(bucket_name, key_name);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			result = false;
		}

		return result;
	}

	public boolean deleteFileList(ArrayList<String> key_name_list) {
		boolean result = true;
		System.out.format("Deleting Files from S3 bucket %s...\n", bucket_name);

		List<KeyVersion> object_keys = key_name_list.stream().map(e -> {
			return new KeyVersion(e);
		}).collect(Collectors.toList());

		try {
			DeleteObjectsRequest dor = new DeleteObjectsRequest(bucket_name).withKeys(object_keys);
			s3.deleteObjects(dor);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			result = false;
		}

		return result;
	}

	/**
	 * This method first deletes all the files in given folder and than the folder
	 * itself
	 */
	public boolean deleteFolder(String folderName) {
		boolean result = true;
		System.out.format("Deleting Folder %s from S3 bucket %s...\n", folderName, bucket_name);
		try {
			s3.deleteObject(bucket_name, folderName + "/");
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			result = false;
		}

		return result;

	}
}
