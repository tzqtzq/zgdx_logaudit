package esLogdeal;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.Date;

public class JudgeDays {

    public SearchResponse changeParameter(int day, String timePeriod1, QueryBuilder queryBuilderDefine, int line, Date date1){

        //获取客户端实例
        TransportClient client = Es_Utils.startupClient();
        SearchResponse response = null;

        switch (day+""){
            case "0":
                response=client.prepareSearch(Es_Utils.INDEX_DEMO_HDFS)
                        .setTypes(timePeriod1)
                        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .setQuery(queryBuilderDefine)
                        .setFrom(0).setSize(line).setExplain(true)
                        .get();
                break;
            case "1":
                response=client.prepareSearch(Es_Utils.INDEX_DEMO_HDFS)
                        .setTypes(timePeriod1,(DiffrentDays.changeDate(date1,1)))
                        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .setQuery(queryBuilderDefine)
                        .setFrom(0).setSize(line).setExplain(true)
                        .get();;
                break;
            case "2":
                response=client.prepareSearch(Es_Utils.INDEX_DEMO_HDFS)
                        .setTypes(timePeriod1,(DiffrentDays.changeDate(date1,1)),(DiffrentDays.changeDate(date1,2)))
                        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .setQuery(queryBuilderDefine)
                        .setFrom(0).setSize(line).setExplain(true)
                        .get();
                break;
            case "3":
                response=client.prepareSearch(Es_Utils.INDEX_DEMO_HDFS)
                        .setTypes(timePeriod1,(DiffrentDays.changeDate(date1,1)),(DiffrentDays.changeDate(date1,2)),(DiffrentDays.changeDate(date1,3)))
                        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .setQuery(queryBuilderDefine)
                        .setFrom(0).setSize(line).setExplain(true)
                        .get();
                break;
            case "4":
                response=client.prepareSearch(Es_Utils.INDEX_DEMO_HDFS)
                        .setTypes(timePeriod1,(DiffrentDays.changeDate(date1,1)),(DiffrentDays.changeDate(date1,2)),(DiffrentDays.changeDate(date1,3)),(DiffrentDays.changeDate(date1,4)))
                        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .setQuery(queryBuilderDefine)
                        .setFrom(0).setSize(line).setExplain(true)
                        .get();
                break;
            case "5":
                response=client.prepareSearch(Es_Utils.INDEX_DEMO_HDFS)
                        .setTypes(timePeriod1,(DiffrentDays.changeDate(date1,1)),(DiffrentDays.changeDate(date1,2)),(DiffrentDays.changeDate(date1,3)),(DiffrentDays.changeDate(date1,4)),(DiffrentDays.changeDate(date1,5)))
                        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .setQuery(queryBuilderDefine)
                        .setFrom(0).setSize(line).setExplain(true)
                        .get();
                break;
            case "6":
                response=client.prepareSearch(Es_Utils.INDEX_DEMO_HDFS)
                        .setTypes(timePeriod1,(DiffrentDays.changeDate(date1,1)),(DiffrentDays.changeDate(date1,2)),(DiffrentDays.changeDate(date1,3)),(DiffrentDays.changeDate(date1,4)),(DiffrentDays.changeDate(date1,5)),(DiffrentDays.changeDate(date1,6)))
                        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .setQuery(queryBuilderDefine)
                        .setFrom(0).setSize(line).setExplain(true)
                        .get();
                break;
            default:
                System.out.println("错误的输入时间");
        }
        return response;
    }

}
