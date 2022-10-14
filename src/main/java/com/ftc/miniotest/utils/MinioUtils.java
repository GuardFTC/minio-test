package com.ftc.miniotest.utils;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.ArrayUtil;
import com.ftc.miniotest.exception.BucketExistException;
import com.ftc.miniotest.exception.BucketNotExistException;
import io.minio.*;
import io.minio.http.Method;
import io.minio.messages.Bucket;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: 冯铁城 [17615007230@163.com]
 * @date: 2022-10-13 14:54:08
 * @describe: minio工具类
 */
@Component
@RequiredArgsConstructor
public class MinioUtils {

    private final MinioClient minioClient;

    /**
     * 判定桶是否存在
     *
     * @param bucketName 桶名称
     * @return 桶是否存在
     */
    @SneakyThrows
    public boolean bucketExists(String bucketName) {

        //1.构建参数
        BucketExistsArgs existsArgs = BucketExistsArgs.builder().bucket(bucketName).build();

        //2.请求返回
        return minioClient.bucketExists(existsArgs);
    }

    /**
     * 创建桶
     *
     * @param bucketName 桶名称
     */
    @SneakyThrows
    public void createBucket(String bucketName) {

        //1.判定桶是否存在
        checkBucketExist(bucketName);

        //2.构建参数
        MakeBucketArgs args = MakeBucketArgs.builder().bucket(bucketName).build();

        //3.创建桶
        minioClient.makeBucket(args);
    }

    /**
     * 删除桶
     *
     * @param bucketName 桶名称
     */
    @SneakyThrows
    public void removeBucket(String bucketName) {

        //1.判定桶是否不存在
        checkBucketNotExist(bucketName);

        //2.构建参数
        RemoveBucketArgs args = RemoveBucketArgs.builder().bucket(bucketName).build();

        //3.删除桶
        minioClient.removeBucket(args);
    }

    /**
     * 列出所有桶信息
     *
     * @return 所有桶信息
     */
    @SneakyThrows
    public List<Bucket> listBuckets() {
        return minioClient.listBuckets();
    }

    /**
     * 上传Multipart文件对象
     *
     * @param bucketName 桶名称
     * @param objectName 对象名称
     * @param file       Multipart文件
     */
    @SneakyThrows
    public void uploadObjectFromMultipartFile(String bucketName, String objectName, MultipartFile file) {

        //1.获取属性
        InputStream inputStream = file.getInputStream();
        long size = file.getSize();
        String contentType = file.getContentType();

        //2.上传文件
        uploadObject(bucketName, objectName, inputStream, size, contentType);
    }

    /**
     * 上传网络文件对象
     *
     * @param bucketName 桶名称
     * @param objectName 对象名称
     * @param objectUrl  文件URL
     */
    @SneakyThrows
    public void uploadObjectFromNetwork(String bucketName, String objectName, String objectUrl) {

        //1.打开文件链接
        URLConnection connection = new URL(objectUrl).openConnection();
        connection.connect();

        //2.上传文件
        uploadObject(bucketName, objectName, connection.getInputStream(), connection.getContentLength(), connection.getContentType());
    }

    /**
     * 上传本地文件对象
     *
     * @param bucketName  桶名称
     * @param objectName  对象名称
     * @param filePath    本地文件路径
     * @param contentType 对象内容类型（用于判定是下载对象/预览对象）
     */
    public void uploadObjectFromLocal(String bucketName, String objectName, String filePath, String contentType) {

        //1.将本地文件读取为字节流
        byte[] bytes = FileUtil.readBytes(filePath);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

        //2.上传文件
        uploadObject(bucketName, objectName, inputStream, bytes.length, contentType);
    }

    /**
     * 上传对象
     *
     * @param bucketName  桶名称
     * @param objectName  对象名称
     * @param inputStream 输入流
     * @param objectSize  对象大小/字节
     * @param contentType 对象内容类型（用于判定是下载对象/预览对象）
     */
    @SneakyThrows
    public void uploadObject(String bucketName, String objectName, InputStream inputStream, long objectSize, String contentType) {

        //1.判定桶是否不存在
        checkBucketNotExist(bucketName);

        //2.构建参数
        PutObjectArgs args = PutObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .stream(inputStream, objectSize, -1)
                .contentType(contentType)
                .build();

        //3.上传文件
        minioClient.putObject(args);
    }

    /**
     * 删除桶中对象
     *
     * @param bucketName  桶名称
     * @param objectNames 对象名称 为空代表删除桶中所有数据
     */
    public void removeObjects(String bucketName, String... objectNames) {

        //1.判定桶是否不存在
        checkBucketNotExist(bucketName);

        //2.定义删除对象集合
        List<DeleteObject> deleteObjects = CollUtil.newArrayList();

        //3.objectName为空，代表删除桶中全部数据
        if (ArrayUtil.isEmpty(objectNames)) {

            //4.查询并解析对象名称
            List<Item> items = listObjectsFromBucket(bucketName);
            List<String> deleteObjectNames = items.stream().map(Item::objectName).collect(Collectors.toList());

            //5.格式转换
            objectNames = Convert.toStrArray(deleteObjectNames);
        }

        //6.封装删除对象集合
        for (String deleteObjectName : objectNames) {
            deleteObjects.add(new DeleteObject(deleteObjectName));
        }

        //7.封装参数
        RemoveObjectsArgs args = RemoveObjectsArgs.builder()
                .bucket(bucketName)
                .objects(deleteObjects)
                .build();

        //8.删除
        Iterable<Result<DeleteError>> results = minioClient.removeObjects(args);
        Assert.isFalse(results.iterator().hasNext());
    }

    /**
     * 列出桶中全部对象
     *
     * @param bucketName 桶名称
     * @return 桶中全部对象
     */
    @SneakyThrows
    public List<Item> listObjectsFromBucket(String bucketName) {

        //1.判定桶是否不存在
        checkBucketNotExist(bucketName);

        //2.构建参数
        ListObjectsArgs args = ListObjectsArgs.builder().bucket(bucketName).build();

        //3.查询
        Iterable<Result<Item>> results = minioClient.listObjects(args);

        //4.封装
        List<Item> items = CollUtil.newArrayList();
        for (Result<Item> result : results) {
            items.add(result.get());
        }

        //5.返回
        return items;
    }

    /**
     * 获取对象
     *
     * @param bucketName 桶名称
     * @param objectName 对象名称
     * @return 对象信息
     */
    @SneakyThrows
    public GetObjectResponse getObject(String bucketName, String objectName) {

        //1.判定桶是否不存在
        checkBucketNotExist(bucketName);

        //2.构建参数
        GetObjectArgs args = GetObjectArgs.builder().bucket(bucketName).object(objectName).build();

        //3.查询并返回
        return minioClient.getObject(args);
    }

    /**
     * 获取对象Url（下载/预览的Url）
     *
     * @param bucketName 桶名称
     * @param objectName 对象名称
     * @return 对象Url
     */
    @SneakyThrows
    public String getObjectUrl(String bucketName, String objectName) {

        //1.判定桶是否不存在
        checkBucketNotExist(bucketName);

        //2.构建参数
        GetPresignedObjectUrlArgs args = GetPresignedObjectUrlArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .method(Method.GET)
                .build();

        //3.获取Url
        String url = minioClient.getPresignedObjectUrl(args);

        //4.Url解码
        url = URLDecoder.decode(url, StandardCharsets.UTF_8.name());

        //5.返回
        return url;
    }

    /**
     * 校验桶存在异常
     *
     * @param bucketName 桶名称
     */
    private void checkBucketExist(String bucketName) {
        if (bucketExists(bucketName)) {
            throw new BucketExistException(bucketName);
        }
    }

    /**
     * 校验桶不存在异常
     *
     * @param bucketName 桶名称
     */
    private void checkBucketNotExist(String bucketName) {
        if (!bucketExists(bucketName)) {
            throw new BucketNotExistException(bucketName);
        }
    }
}
