package nus.learning.cs5425.ImagaRetrival.repository.Imp;

import avro.shaded.com.google.common.base.Joiner;
import com.mongodb.spark.MongoSpark;
import jsat.linear.DenseVector;
import jsat.linear.distancemetrics.CosineDistance;
import nus.learning.cs5425.ImagaRetrival.repository.MongoSparkRepository;
import nus.learning.cs5425.ImagaRetrival.service.ML_Service;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Repository;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.List;

@Repository
public class MongoSparkRepositoryImp implements MongoSparkRepository {
    @Autowired
    private JavaSparkContext jsc;

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private ML_Service ml_service;

    @Override
    public Dataset<Row> loadDataset(boolean cache) {
        Dataset<Row> cluster_df = MongoSpark.load(jsc).toDF();

        cluster_df = cluster_df.withColumn("encd",
                                           functions.from_json(cluster_df.col("encd"), DataTypes.createArrayType(DataTypes.DoubleType)));

        cluster_df.createOrReplaceTempView("cluster_df");
        if (cache){
            this.sparkSession.sqlContext().cacheTable("cluster_df");
            cluster_df = cluster_df.cache();
        }
        return cluster_df;
    }

    @Override
    public Dataset<Row> findByClusterId(Dataset<Row> df, int cluster_id) {
        return df.where(String.format("cluster_id=%d",cluster_id));
    }

    @Override
    public Dataset<Row> computeCosineSimilarity(Dataset<Row> df, List<Double> query_enc) {
//        df.createOrReplaceTempView("df_temp");
        String array_str = Joiner.on(",").join(query_enc);
        Dataset<Row> res = df
                .withColumn("DotProduct",
                        functions.expr(String.format(
                                "aggregate(zip_with(encd, array(%s), (x, y) -> x * y), 0D, " +
                                        "(sum, x) -> sum + x)", array_str)))
                .withColumn("normA",
                        functions.expr(String.format(
                                "aggregate(zip_with(encd, encd, (x, y) -> x * y), 0D, " +
                                        "(sum, x) -> sum + x)")))
                .withColumn("normB",
                        functions.expr(String.format(
                                "aggregate(zip_with(array(%s), array(%s), (x, y) -> x * y), 0D, " +
                                        "(sum, x) -> sum + x)", array_str, array_str)))
                .withColumn("cosine_similarity",
                        functions.expr("DotProduct / (sqrt(normA) * sqrt(normB))"));

        return res.select("id","cluster_id","img_name", "url", "cosine_similarity")
                .orderBy(res.col("cosine_similarity").desc());
    }
}