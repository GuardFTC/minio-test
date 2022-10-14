package com.ftc.miniotest.utils;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.StrUtil;
import com.ftc.miniotest.config.MinioConfig;
import com.ftc.miniotest.exception.BucketExistException;
import com.ftc.miniotest.exception.BucketNotExistException;
import io.minio.GetObjectResponse;
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
class MinioUtilsTest {

    @Autowired
    private MinioUtils minioUtils;

    @Autowired
    private MinioConfig minioConfig;

    @AfterEach
    void removeAll() {

        //1.获取全部桶
        List<Bucket> buckets = minioUtils.listBuckets();

        //2.遍历每个桶
        for (Bucket bucket : buckets) {

            //3.获取桶中全部对象
            List<Item> objects = minioUtils.listObjectsFromBucket(bucket.name());

            //4.对象不为空，删除全部对象
            if (CollUtil.isNotEmpty(objects)) {
                minioUtils.removeObjects(bucket.name());
            }

            //5.删除桶信息
            minioUtils.removeBucket(bucket.name());
        }
    }

    @Test
    void testBucketApi() {

        //1.判定桶是否存在
        boolean bucketExists = minioUtils.bucketExists(minioConfig.getBucket());
        Assert.isFalse(bucketExists);

        //2.创建桶
        minioUtils.createBucket(minioConfig.getBucket());

        //3.判定桶是否存在
        bucketExists = minioUtils.bucketExists(minioConfig.getBucket());
        Assert.isTrue(bucketExists);

        //4.列出桶信息
        List<Bucket> buckets = minioUtils.listBuckets();
        Assert.isTrue(1 == buckets.size());
        Assert.isTrue(minioConfig.getBucket().equals(buckets.get(0).name()));

        //5.删除桶
        minioUtils.removeBucket(minioConfig.getBucket());

        //6.判定桶是否存在
        bucketExists = minioUtils.bucketExists(minioConfig.getBucket());
        Assert.isFalse(bucketExists);
    }

    @Test
    @SneakyThrows(IOException.class)
    void testUploadObjectApi() {

        //1.验证桶是否存在
        boolean bucketExists = minioUtils.bucketExists(minioConfig.getBucket());
        if (!bucketExists) {

            //2.不存在创建桶
            minioUtils.createBucket(minioConfig.getBucket());

            //3.验证桶创建成功
            bucketExists = minioUtils.bucketExists(minioConfig.getBucket());
            Assert.isTrue(bucketExists);
        }

        //4.上传本地文件
        String filePath = "D:\\Downloads\\MicrosoftEdge\\286700946_560240985619390_357231379613308151_n.jpg";
        String objectName = "test_1.jpg";
        String contentType = "image/jpeg";
        minioUtils.uploadObjectFromLocal(minioConfig.getBucket(), objectName, filePath, contentType);

        //5.获取文件Url
        String objectUrl = minioUtils.getObjectUrl(minioConfig.getBucket(), objectName);
        Assert.isTrue(StrUtil.isNotBlank(objectUrl));

        //6.上传网络文件
        String url = "https://gimg2.baidu.com/image_search/src=http%3A%2F%2Fc-ssl.duitang.com%2Fuploads%2Fblog%2F202101%2F24%2F20210124205350_bf8ee.jpeg&refer=http%3A%2F%2Fc-ssl.duitang.com&app=2002&size=f9999,10000&q=a80&n=0&g=0n&fmt=auto?sec=1668306839&t=dc897d83f273132a9e6f6807fae11cea";
        objectName = "test_2.jpg";
        minioUtils.uploadObjectFromNetwork(minioConfig.getBucket(), objectName, url);

        //7.获取文件Url
        objectUrl = minioUtils.getObjectUrl(minioConfig.getBucket(), objectName);
        Assert.isTrue(StrUtil.isNotBlank(objectUrl));

        //8.上传MultipartFile
        filePath = "D:\\Downloads\\MicrosoftEdge\\309498853_787915615775319_4117145053054472390_n.jpg";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(FileUtil.readBytes(filePath));

        MockMultipartFile mockMultipartFile = new MockMultipartFile("1.jpg", "1.jpg", contentType, inputStream);
        objectName = "test_3.jpg";
        minioUtils.uploadObjectFromMultipartFile(minioConfig.getBucket(), objectName, mockMultipartFile);

        //9.获取文件Url
        objectUrl = minioUtils.getObjectUrl(minioConfig.getBucket(), objectName);
        Assert.isTrue(StrUtil.isNotBlank(objectUrl));
    }

    @Test
    void testObjectApi() {

        //1.上传文件
        testUploadObjectApi();

        //2.获取桶中全部对象
        List<Item> items = minioUtils.listObjectsFromBucket(minioConfig.getBucket());
        Assert.isTrue(3 == items.size());

        //3.随机删除一个对象
        Item item = items.get(0);
        minioUtils.removeObjects(minioConfig.getBucket(), item.objectName());

        //4.获取桶中全部对象
        items = minioUtils.listObjectsFromBucket(minioConfig.getBucket());
        Assert.isTrue(2 == items.size());
        Assert.isTrue(!items.stream().map(Item::objectName).collect(Collectors.toList()).contains(item.objectName()));

        //5.删除桶中剩下两个对象
        minioUtils.removeObjects(minioConfig.getBucket(), items.get(0).objectName(), items.get(1).objectName());

        //6.获取桶中全部对象
        items = minioUtils.listObjectsFromBucket(minioConfig.getBucket());
        Assert.isTrue(0 == items.size());

        //7.上传文件
        testUploadObjectApi();

        //8.删除桶中全部对象
        minioUtils.removeObjects(minioConfig.getBucket());

        //9.获取桶中全部对象
        items = minioUtils.listObjectsFromBucket(minioConfig.getBucket());
        Assert.isTrue(0 == items.size());

        //10.上传文件
        testUploadObjectApi();

        //11.获取桶中全部对象
        items = minioUtils.listObjectsFromBucket(minioConfig.getBucket());
        Assert.isTrue(3 == items.size());

        //12.查询文件
        GetObjectResponse object = minioUtils.getObject(minioConfig.getBucket(), items.get(0).objectName());
        Assert.isTrue(object.object().equals(items.get(0).objectName()));
        Assert.isTrue(object.bucket().equals(minioConfig.getBucket()));

        //13.将查询的文件写入本地
        File file = FileUtil.writeFromStream(object, "C:\\Users\\86176\\Desktop\\1.jpg");
        Assert.isTrue(file.exists());
    }

    @Test
    void testBucketNotExist() {

        //1.期望抛出异常
        BucketNotExistException exception = assertThrows(BucketNotExistException.class,
                () -> minioUtils.getObject("undefined", "test_3.jpg")
        );

        //2.校验异常信息
        String errorMessage = "undefined对象桶不存在!";
        Assert.isTrue(exception.getMessage().equals(errorMessage));
    }

    @Test
    void testBucketExist() {

        //1.创建桶
        minioUtils.createBucket(minioConfig.getBucket());

        //2.再次创建桶
        BucketExistException exception = assertThrows(BucketExistException.class,
                () -> minioUtils.createBucket(minioConfig.getBucket())
        );

        //3.校验异常信息
        String errorMessage = "test对象桶已存在!";
        Assert.isTrue(exception.getMessage().equals(errorMessage));
    }
}