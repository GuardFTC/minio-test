package com.ftc.miniotest.exception;

import cn.hutool.core.util.StrUtil;

/**
 * @author: 冯铁城 [17615007230@163.com]
 * @date: 2022-10-14 14:37:19
 * @describe: 桶不存在异常
 */
public class BucketNotExistException extends RuntimeException {

    public BucketNotExistException(String bucketName) {
        super(StrUtil.format("{}对象桶不存在!", bucketName));
    }
}
