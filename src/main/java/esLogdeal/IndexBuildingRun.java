package esLogdeal;


public class IndexBuildingRun {

    public static void main(String[] args) {

        //判断数据参数个数是否符合
        if(args.length!=1) {
            System.out.println("参数个数不符合要求");
            System.exit(0);
        }

        String[] splited = args[0].split(",");


        try {

//            Es_BuildIndex.buildIndexMapping(splited[0]+"_logaudit",splited[1]);
            Es_BuildIndex.buildIndexMapping(splited[0]+"_logaudit_test",splited[1]);
            Thread.sleep(3000);
            //获取所有索引名称
            Es_Utils.getAllIndices();
            //为索引建立别名
            Es_BuildIndex.createAliases(splited[0]+"_logaudit_test",splited[0]+"_logaudit_test_V1");
            //查询别名是否创建成功
//            Es_BuildIndex.aliasesExist("logaudit_servicetransfer_V1");
        }catch (Exception e){
            System.out.println(e);
            System.out.println("索引建立失败.......");
        }
    }
}
