import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import static org.apache.spark.sql.functions.callUDF;

import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.mutable.WrappedArray;


import java.util.*;

public class FindPath {
    // From: https://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude
    private static double distance(double lat1, double lat2, double lon1, double lon2) {
        final int R = 6371; // Radius of the earth
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters
        double height = 0; // For this assignment, we assume all locations have the same height.
        distance = Math.pow(distance, 2) + Math.pow(height, 2);
        return Math.sqrt(distance);
    }

//    private static GraphFrame create_transport_graph(String OSM_file):

    /* Define & register UDFS */
    private static void register_UDFs(SQLContext sqlContext){
        sqlContext.udf().register("checkOneWay", new UDF1<WrappedArray<Row>, Boolean>(){
            private static final long serialVersionUID = -5372447039252716846L;
            @Override
            public Boolean call(WrappedArray<Row> tags) throws Exception {
               for(int i=0; i<tags.size(); i++){
                   Row tag = tags.apply(i);
                   if(tag.getString(0).equals("oneway") & tag.getString(1).equals("yes")){
                       return true;
                   }
               }
                return false;
            }
        }, DataTypes.BooleanType);

//        sqlContext.udf().register("nodeLinks", new UDF1<WrappedArray<Row>, List>(){
//            @Override
//            public List call(WrappedArray<Row> nodes) throws Exception {
//                return distance(lat1, lat2, lon1, lon2);
//            }
//        }, DataTypes.);

        sqlContext.udf().register("computeDistance", new UDF4<Double, Double, Double, Double, Double>(){
            @Override
            public Double call(Double lat1, Double lat2, Double lon1, Double lon2) throws Exception {
                return distance(lat1, lat2, lon1, lon2);
            }
        }, DataTypes.DoubleType);
    }

    /*Define global variables */
    static boolean runOnCluster = false;
    static SparkConf sparkConf = new SparkConf().setAppName("FindPath");

    public static void main(String[] args) {
        if (!runOnCluster) {
            sparkConf.setMaster("local[2]");
            sparkConf.setJars(new String[] { "target/eduonix_spark-deploy.jar" });
//            spark = SparkSession.builder().config(sparkConf).getOrCreate();
        }
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);
        String raw_osm_file = args[0];
        register_UDFs(sqlContext);

        Map<String, String> options = new HashMap<>();
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("_id", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("_lat", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("_lon", DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(fields);
        options.put("rowTag", "node");
        Dataset<Row> nodeDF = sqlContext.read()
                .options(options)
                .format("xml")
                .schema(schema)
                .load(raw_osm_file)
                .withColumnRenamed("_id", "node_id")
                .withColumnRenamed("_lat", "latitude")
                .withColumnRenamed("_lon", "longitude").cache();
        nodeDF.createOrReplaceTempView("nodeDF");
//        Dataset<Row> nodeDF = sqlContext.sql("select _id as node_id, _lat as latitude, _lon as longitude from nodeDF");
////        nodeDF.show(10);

        fields = new ArrayList<StructField>(); //reset fields for ways
        fields.add(DataTypes.createStructField("_id", DataTypes.LongType, true));
        List<StructField> inner_fields = new ArrayList<StructField>();
        inner_fields.add(DataTypes.createStructField("_ref", DataTypes.LongType, true));
        ArrayType nd_type = DataTypes.createArrayType(DataTypes.createStructType(inner_fields));
        fields.add(DataTypes.createStructField("nd", nd_type, true));
        inner_fields = new ArrayList<StructField>();
        inner_fields.add(DataTypes.createStructField("_k", DataTypes.StringType, true));
        inner_fields.add(DataTypes.createStructField("_v", DataTypes.StringType, true));
        ArrayType tag_type = DataTypes.createArrayType(DataTypes.createStructType(inner_fields));
        fields.add(DataTypes.createStructField("tag", tag_type, true));
        schema = DataTypes.createStructType(fields);
        options.put("rowTag", "way");
        Dataset<Row> roadDF = sqlContext.read()
                .options(options)
                .format("xml")
                .schema(schema)
                .load(raw_osm_file)
                .withColumnRenamed("_id", "road_id").filter(new FilterFunction<Row>() {
                    @Override
                    public boolean call(Row row) throws Exception {
                        return row.getAs("tag").toString().contains("highway");
                    }
                });
        roadDF = roadDF.withColumn("oneway", callUDF("checkOneWay", roadDF.col("tag")));
        roadDF.show(5);

        List<StructField> listOfStructField=new ArrayList<StructField>();
        listOfStructField.add(DataTypes.createStructField("src", DataTypes.LongType, true));
        listOfStructField.add(DataTypes.createStructField("dest", DataTypes.LongType, true));
        listOfStructField.add(DataTypes.createStructField("road_id", DataTypes.LongType, true));
        StructType relation_schema = DataTypes.createStructType(listOfStructField);
        ExpressionEncoder<Row> encoder = RowEncoder.apply(relation_schema);

        Dataset<String> test = roadDF.map(new MapFunction<Row, Dataset<Row>>() {
            @Override
            public Dataset<Row> call(Row row) throws Exception {
                List<Row> way_relations=new ArrayList<Row>();
                List<Row> nds = row.getList(row.fieldIndex("nd"));
                for(int i=0; i < nds.size()-1; i++){
                    way_relations.add(RowFactory.create(nds.get(i).getLong(0), nds.get(i+1).getLong(0), row.getLong(row.fieldIndex("road_id")))); // src, dest, road_id,
                }
                if(!row.getBoolean(row.fieldIndex("oneway"))){
                    for(int i=nds.size()-1; i > 0; i--){
                        way_relations.add(RowFactory.create(nds.get(i).getLong(0), nds.get(i-1).getLong(0), row.getLong(row.fieldIndex("road_id")))); // src, dest, road_id,
                    }
                }
                return sqlContext.createDataFrame(way_relations, relation_schema);
            }
        }, encoder);

        test.show(5);

        roadDF.createOrReplaceTempView("roadDF");

//        Dataset<Row> p1_expwayNodeDF = sqlContext.sql(
//                "select road_id, explode(nd) as indexedNode, oneway from roadDF");

//        Dataset<Row> p1_expwayNodeDF = sqlContext.sql(
//                "with expended_way as (select road_id, explode(nd) as indexedNode, oneway from roadDF)" +
//                        "");
//        p1_expwayNodeDF.show(5);

//        Dataset<Row> highwayDF = roadData.filter(new FilterFunction<Row>() {
//            @Override
//            public boolean call(Row row) throws Exception {
//                return row.getAs("tag").toString().contains("highway");
//            }
//        }).withColumn("oneway", callUDF("checkOneWay", roadData.col("tag")));
//
//        highwayDF.show(5);


        jsc.stop();
    }
}