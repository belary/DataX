package com.alibaba.datax.com.alibaba.datax.plughin.reader.hivejdbcreader;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


/**
 * @author fanchao on 2019/10/18.
 * @version v0.1
 * @desc hive jdbc reader
 */

public class S3Util {

    private static AmazonS3 s3Client;
    private static Regions clientRegion;
    private static String bucketName;
    private static final Logger LOG = LoggerFactory.getLogger(S3Util.class);

    // 使用环境变量连接aws(~/.aws/config, credentials)
    static
    {
        try {
            clientRegion = Regions.CN_NORTHWEST_1;
            bucketName = "data-archive-ningxia";
            s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new ProfileCredentialsProvider())
                    .withRegion(clientRegion)
                    .build();

            LOG.info("s3 连接成功");
        }
        catch (AmazonServiceException e) {
            e.printStackTrace();
            LOG.error("s3 连接失败");
        } catch (SdkClientException e) {
            e.printStackTrace();
            LOG.error("s3 连接失败");
        }
    }

    // 获取存储对象列表
    // 默认目前的S3桶位于宁夏 可能以后会发生改变
    // todo: bucket走配置文件
    public static ObjectListing listObj(String prefix){

        ObjectListing list = s3Client.listObjects(new ListObjectsRequest()
                .withBucketName(bucketName)
                .withPrefix(prefix));
        return list;
    }

    // 上传本地文件到s3
    // 默认上传分段为5MB
    public static void uploadObj(String keyName, String filePath) {

        String  stringObjKeyName = keyName;
        File file = new File(filePath);
        long contentLength = file.length();
        long partSize = 5 * 1024 * 1024; // 分段大小设置为 5 MB.

        try {

            s3Client.putObject(bucketName, stringObjKeyName, "Uploaded String Object");

            // 创建一个ETag对象的列表。为上载的每个对象部分检索ETag，
            // 将ETag列表传递给请求以完成上载。
            List<PartETag> partETags = new ArrayList<PartETag>();

            // 初始化分段上传
            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, keyName);
            InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);

            // 分段上传开始
            long filePosition = 0;
            for (int i = 1; filePosition < contentLength; i++) {
                // 若分段大小不足上传块设置（5MB）则对上传大小设置为实际大小
                partSize = Math.min(partSize, (contentLength - filePosition));

                // 创建上传请求
                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(bucketName)
                        .withKey(keyName)
                        .withUploadId(initResponse.getUploadId())
                        .withPartNumber(i)
                        .withFileOffset(filePosition)
                        .withFile(file)
                        .withPartSize(partSize);

                // 上传分段 并将s3响应的ETag添加到etag列表中。
                UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
                partETags.add(uploadResult.getPartETag());

                filePosition += partSize;
            }

            // 完成切分段上传
            CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucketName, keyName,
                    initResponse.getUploadId(), partETags);
            s3Client.completeMultipartUpload(compRequest);
            LOG.info("上传成功完成");
        } catch (AmazonServiceException e) {
            //s3无法处理将返回错误信息
            e.printStackTrace();
        } catch (SdkClientException e) {
            // s3 无法连接 或者客户端不能解析从s3返回到响应
            e.printStackTrace();
        }
    }

}