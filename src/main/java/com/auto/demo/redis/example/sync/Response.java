package com.auto.demo.redis.example.sync;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author wbs
 * @date 2020/11/4
 */
@Data
@Accessors(chain = true)
public class Response {
    private Long id;
    private String content;
}
