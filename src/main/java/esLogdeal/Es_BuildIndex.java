package esLogdeal;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tzq on 2018-01-15.
 * <p>
 * mapping创建
 * 添加记录到es
 */
public class Es_BuildIndex {


    /**
     * 索引的mapping
     * <p>
     * 预定义一个索引的mapping,使用mapping的好处是可以个性的设置某个字段等的属性
     * Es_Setting.INDEX_DEMO_01类似于数据库
     * mapping 类似于预设某个表的字段类型
     * <p>
     * Mapping,就是对索引库中索引的字段名及其数据类型进行定义，类似于关系数据库中表建立时要定义字段名及其数据类型那样，
     * 不过es的 mapping比数据库灵活很多，它可以动态添加字段。
     * 一般不需要要指定mapping都可以，因为es会自动根据数据格式定义它的类型，
     * 如果你需要对某 些字段添加特殊属性（如：定义使用其它分词器、是否分词、是否存储等），就必须手动添加mapping。
     * 有两种添加mapping的方法，一种是定义在配 置文件中，一种是运行时手动提交mapping，两种选一种就行了。
     *
     * @throws Exception Exception
     */
    protected static void buildIndexMapping(String index,String type) throws Exception {
        Map<String, Object> settings = new HashMap<>();
        settings.put("number_of_shards", 3);//分片数量
        settings.put("number_of_replicas", 2);//复制数量
        settings.put("refresh_interval", "10s");//刷新时间

        //在本例中主要得注意,ttl及timestamp如何用java ,这些字段的具体含义,请去到es官网查看
        TransportClient client = Es_Utils.startupClient();
//        TransportClient client = GetConnection.getInstance();
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("dynamic", "true").startObject("properties")
                .startObject("usernName").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                .startObject("opTime").field("type", "string").field("index", "not_analyzed").endObject()
                .startObject("fsNamesystem").field("type", "string").field("index", "not_analyzed").endObject()
                .startObject("cmd").field("index", "not_analyzed").field("type", "string").endObject()
                .startObject("ip").field("type", "string")
                            .field("store", "yes")
                            .field("index", "not_analyzed")
                             .endObject()//建立索引但是不分词
                .startObject("src").field("type", "string")
                            .field("store", "yes")
                             .field("index", "not_analyzed")
//                            .field("analyzer", "ik_smart")
                .startObject("dst").field("type", "string")
                .field("store", "yes")
                .field("index", "not_analyzed")
                .startObject("perm").field("type", "string")
                .field("store", "yes")
                .field("index", "not_analyzed")
                .startObject("proto").field("type", "string")
                .field("store", "yes")
                .field("index", "not_analyzed")
                .endObject()
                .endObject()
                .endObject();
        CreateIndexRequestBuilder cib = client.admin().indices().prepareCreate(index);
        cib.setSettings(settings);
        cib.addMapping(type, builder).get();
//        client.close();
    }

    /**
     * 给 []index 创建别名
     * 重载方法可以按照过滤器或者Query 作为一个别名
     *
     * @param aliases aliases别名
     * @param indices 多个 index
     * @return 是否完成
     */
    protected static boolean createAliases(String aliases, String... indices) {
        TransportClient client = Es_Utils.startupClient();
        IndicesAliasesRequestBuilder builder = client.admin().indices().prepareAliases();
        return builder.addAlias(indices, aliases).execute().isDone();
    }

    /**
     * 查询此别名是否存在
     *
     * @param aliases aliases
     * @return 是否存在
     */
    protected static boolean aliasesExist(String... aliases) {
        TransportClient client = Es_Utils.startupClient();
        AliasesExistRequestBuilder builder =
                client.admin().indices().prepareAliasesExist(aliases);
        AliasesExistResponse response = builder.execute().actionGet();
        return response.isExists();
    }

    /**
     * 添加记录到es
     * <p>
     * 增加索引记录
     *
     * @param user 添加的记录
     * @throws Exception Exception
     */
//    protected static void buildIndex(User user) throws Exception {
//        // INDEX_DEMO_01_MAPPING为上个方法中定义的索引,prindextype为类型.jk8231为id,以此可以代替memchche来进行数据的缓存
//        IndexResponse response = Es_Utils.client.prepareIndex(Es_Utils.LOG_STASH_YYYY_MM_DD, Es_Utils.LOG_STASH_YYYY_MM_DD_MAPPING)
//                .setSource(
//                        User.getXContentBuilder(user)
//                )
//                .setTTL(8000)//这样就等于单独设定了该条记录的失效时间,单位是毫秒,必须在mapping中打开_ttl的设置开关
//                .execute()
//                .actionGet();
//    }

    /**
     * 批量添加记录到索引
     *
//     * @param userList 批量添加数据
     * @throws IOException IOException
     */
//    protected static void buildBulkIndex(List<User> userList) throws IOException {
//        BulkRequestBuilder bulkRequest = Es_Utils.client.prepareBulk();
//        // either use Es_Setting.client#prepare, or use Requests# to directly build index/delete requests
//
//        for (User user : userList) {
//            //通过add批量添加
//            bulkRequest.add(Es_Utils.client.prepareIndex(Es_Utils.LOG_STASH_YYYY_MM_DD, Es_Utils.LOG_STASH_YYYY_MM_DD_MAPPING)
//                    .setSource(
//                            User.getXContentBuilder(user)
//                    )
//            );
//        }
//
//        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
//        //如果失败
//        if (bulkResponse.hasFailures()) {
//            // process failures by iterating through each bulk response item
//            System.out.println("buildFailureMessage:" + bulkResponse.buildFailureMessage());
//        }
//    }


    public static void main(String[] args) throws Exception {
//        Es_Utils.startupClient();

//        buildIndexMapping();
    }


}
