package esLogdeal;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class ES_SearchRequest {

    public static void main(String[] args) {
        String[] split = args[0].split(",");
        DealRequest(split[0],split[1],split[2],split[3],split[4],split[5], Integer.parseInt(split[6]));
    }

    //line参数指的是返回的条数 限制数为10000；
    public static void DealRequest(String userName,String serviceName,String ip,String result,String timePeriod1,String timePeriod2,int line){



        try {
            //添加查询添加
            QueryBuilder queryBuilderDefine=QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("ip",ip))
                    .filter(QueryBuilders.termQuery("usernName", userName))
                    .filter(QueryBuilders.termQuery("result", result))
                    .filter(QueryBuilders.termQuery("serviceName",serviceName));

            //获取时间工具的实例
            DiffrentDays difDay = new DiffrentDays();

            //定义type
            String temporaryType="";
            Date date1=new SimpleDateFormat("yyyyMMdd").parse(timePeriod1);
            Date date2=new SimpleDateFormat("yyyyMMdd").parse(timePeriod2);
            //根据输入参数确定查询几天的数据
            int day = DiffrentDays.differentDays(date1, date2);

            SearchResponse response = new JudgeDays().changeParameter(day, timePeriod1, queryBuilderDefine, line, date1);

            System.out.println(temporaryType);

            //Web参数时间段，对应ES type，将七天内的数据注册成表

            ArrayList<Map> resultList = new ArrayList<>();

            for (SearchHit hits : response.getHits()){

                Map<String, Object> source = hits.getSource();
                //将返回结果装到list中返回
                resultList.add(source);
                System.out.println(source);
            }
        }catch (Exception e){
            System.out.println(Exception.class);
        }

    }
}
