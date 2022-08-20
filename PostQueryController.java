package nus.learning.cs5425.ImagaRetrival.controller;

import nus.learning.cs5425.ImagaRetrival.domain.ClusteredMetaData;
import nus.learning.cs5425.ImagaRetrival.domain.FastApiResponse;
import nus.learning.cs5425.ImagaRetrival.domain.QueryRequest;
import nus.learning.cs5425.ImagaRetrival.domain.RedisEncData;
import nus.learning.cs5425.ImagaRetrival.service.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import scala.Serializable;

import javax.validation.Valid;
import java.util.*;
import java.util.stream.Collectors;

@Controller
@CrossOrigin
public class PostQueryController {

    @Autowired
    private JavaSparkContext jsc;
    
    @Autowired
    private FastapiClientService fastapiClientService;

    @Autowired
    private RedisService redisService;

    @Autowired
    private ClusteredMetaDataService clusteredMetaDataService;

    @Autowired
    private MongoSparkService mongoSparkService;

    @Autowired
    private ML_Service ml_service;

    private int K = 10;

    private boolean is_spark = true;
    private Dataset<Row> metadata = null;


    private static class RunTimeReport implements Serializable {
        private static final String BOUNDARY_LINE = "========================================\n";
        private String Report = "\n"+BOUNDARY_LINE+"CS5424 project Runtime report\n"+BOUNDARY_LINE;
        private long start;

        RunTimeReport(long start){
            this.start = start;
        }

        public void setStart(long start) {
            this.start = start;
        }

        private Float getRunTime(){
            long end = System.currentTimeMillis();
            return (end - this.start) / 1000F;
        }
        private void addInfo(String info, Boolean milestone){
            this.Report += info + "\n";
            if(milestone) {
                this.Report +=String.format("Runtime: %f seconds\n", getRunTime());
                this.Report += BOUNDARY_LINE;
            }
        }
        private String getReport(){
            return this.Report;
        }
        private void printReport(){
            System.out.println(this.Report);
        }
    }


    @GetMapping("/send_queries")
    public String collectQueries(Model model) {
        model.addAttribute("request", new QueryRequest());
        return "send_queries";
    }


    @RequestMapping(value = "/send_queries", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Map<String, List<Map<String, String>>> collectQueriesToFastapi(@Valid @ModelAttribute("request")QueryRequest request){
        FastApiResponse response = new FastApiResponse();
        if (request.getReset()!=null){
            fastapiClientService.clearRedis();
//            model.addAttribute("result","redis cleared");
        }
        if (request.getQueries()!=""){
            RunTimeReport run_time_report = null;
            List<String> queriesCollection = Arrays.asList(request.getQueries().split("\n"));
            long startTime = System.currentTimeMillis();
            response = fastapiClientService.sendQueriesToRedis(queriesCollection);
            HashMap<String, List<ClusteredMetaData>> TopK_res = new HashMap<String, List<ClusteredMetaData>>();
            if (this.is_spark) {
                this.metadata = this.mongoSparkService.loadDataset(false);
            }
            if (response.getState().equals("success")) {
                long Time_cursor1 = System.currentTimeMillis();
                run_time_report = new RunTimeReport(System.currentTimeMillis());
                run_time_report.addInfo("Encodings sent to redis.", true);
                for (String q:queriesCollection){
                    int k = 0;
                    RedisEncData EncDataRes = redisService.getEncDataByKey(q);
                    Integer cluster = EncDataRes.getCluster().intValue();
                    run_time_report.setStart(System.currentTimeMillis());
                    run_time_report.addInfo("Processing query", false);
                    run_time_report.addInfo(q, false);
                    run_time_report.addInfo(String.format("cluster = %d", cluster), false);

                    if (this.is_spark){
                        Dataset<Row> clustered_df = this.mongoSparkService.findByClusterId(this.metadata, cluster);
                        Dataset<Row> clustered_df_sim =this.mongoSparkService.computeCosineSimilarity(clustered_df, EncDataRes.getEncoding());
                        List<Row> res_list = clustered_df_sim.limit(this.K).collectAsList();
                        for (Row row: res_list){
                            ClusteredMetaData res_row = new ClusteredMetaData(row.getString(0), row.getInt(1), null, null, row.getString(3), null, row.getString(2));
                            res_row.setSimilarity(row.getDouble(4));
                            if (TopK_res.containsKey(q)){
                                List<ClusteredMetaData> res = TopK_res.get(q);
                                res.add(res_row);
                                TopK_res.put(q,res);
                            } else {
                                TopK_res.put(q, new ArrayList<ClusteredMetaData>() {
                                    { add(res_row); }
                                });
                            }
                        }
                        String format = String.format("Request of item: belongs to cluster: %d.", cluster);
                        run_time_report.addInfo("Complete query", false);
                        run_time_report.addInfo(format, true);
                    } else {
                        List<ClusteredMetaData> metaDataFromCluster = this.clusteredMetaDataService.findByClusterId(cluster);
                        for (ClusteredMetaData metaData: metaDataFromCluster){
                            metaData.setSimilarity(ml_service.CosineSimilarity(EncDataRes.getEncoding(), metaData.getEncd()));
                            if (TopK_res.containsKey(q)){
                                List<ClusteredMetaData> res = TopK_res.get(q);
                                res.add(metaData);
                                Collections.sort(res, Comparator.comparing(ClusteredMetaData::getSimilarity).reversed());
                                TopK_res.put(q, res.stream().limit(this.K).collect(Collectors.toList()));
                            } else {
                                TopK_res.put(q, new ArrayList<ClusteredMetaData>() {
                                    { add(metaData); }
                                });
                            }
                        }
                    }

                }
                StringBuilder final_res = new StringBuilder("\n");
                Long TimeTaken = (System.currentTimeMillis()-Time_cursor1)/1000;
                final_res.append(String.format("<h3 style=\"text-align: center;color: #FB667A;\"> Number of queries = %d; Time taken = %s s </h3>\n", TopK_res.size(),  TimeTaken.toString()));
                for (Map.Entry<String, List<ClusteredMetaData>> entry : TopK_res.entrySet()) {
//                    final_res+=entry.getKey()+":\n";
                    final_res.append(String.format("<h3> %s</h3>\n", entry.getKey()));
                    final_res.append("<div class=\"row\">");
                    for (ClusteredMetaData ele : entry.getValue()){
                        final_res.append("<div class=column><figure>");
                        final_res.append("<img src="+ele.getUrl()+" alt="+ele.getImg_name()+" style=\"width:350px;height:180px\">");
                        final_res.append("<figcaption> <h4>"+ele.getSimilarity().toString()+" </h4></figcaption>");
                        final_res.append("</figure></div>");
//                        final_res+="img_"+ele.getImg_name()+" = "+ele.getSimilarity().toString()+"\n";
//                        final_res+="url = "+ele.getUrl()+"\n";
                    }
                    final_res.append("</div>");
                    final_res.append("<h3>================================================================</h3>");
                }
//                model.addAttribute("result", final_res.toString());

                return Collections.singletonMap("data", TopK_res.values()
                                                                        .stream()
                                                                        .findFirst()
                                                                        .get()
                                                                        .stream()
                                                                        .map(v -> {
                                                                            Map<String, String> data = new HashMap<>();
                                                                            data.put("url", v.getUrl());
                                                                            data.put("score", v.getSimilarity().toString());
                                                                            return data;
                                                                        })
                                                                        .collect(Collectors.toList()));
            } else {
//                model.addAttribute("result",response.getState());
            }
//            jsc.stop();
            run_time_report.printReport();

        }
//        return "send_queries";
        return Collections.singletonMap("data", new ArrayList<>());
    }
}
