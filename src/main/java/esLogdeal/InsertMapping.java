package esLogdeal;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class InsertMapping {

    public  void insertData(String[] args) {

        TransportClient client = Es_Utils.startupClient();

        //处理python脚本从ftp上拉下来的数据路径
//        String path="/home/hadoopadmin/servicedata/"+type;
//        String index = "logaudit_servicetransfer_real";

//        File file = new File(path);
//        File[] filePath = file.listFiles();

        BulkRequestBuilder bulkRequest = client.prepareBulk();

        //从文件中读取数据

        try {

//            BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\tianzhongqiu\\Desktop\\test1.txt"));
//            BufferedReader br = new BufferedReader(new FileReader(filePath[0]));

            String type = new SimpleDateFormat("yyyyMMdd").format(new Date());
            String line = null;
            long i =0L;
            while (true) {

                System.out.println(i);
                i++;
                String[] split = line.split("[|]");
                Map<String, Object> ret = new HashMap<String, Object>();
                ret.put("serviceName", split[1]);
                ret.put("usernName", split[2]);
                ret.put("opTime", split[3]);
                ret.put("wastTime", split[4]);
                ret.put("ip", split[5]);
                ret.put("result", split[6]);
                bulkRequest.add(client.prepareIndex(Es_Utils.INDEX_DEMO_HDFS, type).setSource(ret));
//                bulkRequest.add(client.prepareIndex("evaluationtest1", "woman").setSource(ret));
                // 每20000条提交一次
                if (i % 100000 == 0) {
                    System.out.println("输入行数 "+ i +"********");
                    BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                    bulkRequest = client.prepareBulk();
                    if (bulkResponse.hasFailures()) {
                // process failures by iterating through each bulk response item
                System.out.println("buildFailureMessage:" + bulkResponse.buildFailureMessage());
                    }
                }
            }
                //这个是防止读取数据不到100000条但是读完了 要提交一个没有装满的桶
//            bulkRequest.execute().actionGet();


        }catch (Exception e){
            System.out.println(e);
        }
    }
}
