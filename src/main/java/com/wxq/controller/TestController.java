package com.wxq.controller;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author wangxiaoqiang
 * @date 2020/5/25 13:14
 * @description
 */
@RestController
public class TestController {
    @Resource
    private RestHighLevelClient client;

    @RequestMapping("/test")
    public String test(){

        return "hello";
    }

    @RequestMapping("index")
    public String index() throws IOException {
        String json = "{" +
                "\"user\":\"u"+getInteger()+"\"," +
                "\"postDate\":\"2020-05-25\"," +
                "\"message\":\"info"+getInteger()+"\"" +
                "}";
        IndexRequest request = new IndexRequest();
        request.index("test");
        request.id("123"+getInteger()+getInteger()+getInteger()+getInteger()+getInteger()+getInteger()+getInteger()+getInteger()+getInteger());

        request.source(json,XContentType.JSON);
        IndexResponse index = client.index(request, RequestOptions.DEFAULT);


        return index.toString();
    }

    @RequestMapping("get")
    public Object get(){
        SearchRequest request = new SearchRequest("test");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        request.source(searchSourceBuilder);
        try {
            SearchResponse search = client.search(request, RequestOptions.DEFAULT);
            SearchHits hits = search.getHits();
            SearchHit[] hits1 = hits.getHits();
            List<Map<String, Object>> list = new ArrayList<>();
            for(SearchHit hit:hits1){
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                list.add(sourceAsMap);
            }
            return list;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @RequestMapping("scroll")
    public Object scroll(){
        SearchRequest request = new SearchRequest("test");
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        request.scroll(new TimeValue(60000));
        return null;
    }


    private int getInteger(){
        return (int) (Math.random()*100);
    }

}
