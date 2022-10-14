package com.ftc.miniotest.exception;

import cn.hutool.core.util.StrUtil;

/**
 * @author: 冯铁城 [17615007230@163.com]
 * @date: 2022-10-14 14:37:19
 * @describe: 桶存在异常
 */
public class BucketExistException extends RuntimeException {

    public BucketExistException(String bucketName) {
        super(StrUtil.format("{}对象桶已存在!", bucketName));
    }
}
