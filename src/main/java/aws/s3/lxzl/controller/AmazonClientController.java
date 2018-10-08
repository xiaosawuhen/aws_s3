package aws.s3.lxzl.controller;

import java.io.File;
import java.util.List;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import aws.s3.lxzl.bean.AmazonResult;
import aws.s3.lxzl.client.S3Client;

@RestController
public class AmazonClientController {

	@RequestMapping("/startAmazon")
	public AmazonResult start() {
		AmazonResult result = new AmazonResult();
		
		
		File file = new File("C:\\Users\\nannan.c.wang\\Downloads\\20171229173606486.png");
		
		S3Client client = new S3Client("ap-northeast-1")
		.changeBucket("test-for-lsh-file-server");
		
		client.uploadFile("uploadFile_03", file);
		
		List<S3ObjectSummary> objects = client.listFiles();
		for (S3ObjectSummary os: objects) {
		    System.out.println("* " + os.getKey());
		}
		
		client.downloadFile("test.txt", "C:\\Users\\nannan.c.wang\\Desktop");
		
		result.setResult("success");
		return result;
	}
}
