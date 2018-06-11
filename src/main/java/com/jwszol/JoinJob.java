package com.jwszol;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;
import scala.Tuple3;

import com.google.common.base.Optional;

/**
 * Created by kubaw on 29/05/17.
 */
public class JoinJob {

    private static JavaSparkContext sc;

    public JoinJob(JavaSparkContext sc){
        this.sc = sc;
    }

    public static final PairFunction<Tuple2<Integer, Optional<String>>, Integer, String> KEY_VALUE_PAIRER =
            new PairFunction<Tuple2<Integer, Optional<String>>, Integer, String>() {
                public Tuple2<Integer, String> call(
                        Tuple2<Integer, Optional<String>> a) throws Exception {
                    // a._2.isPresent()
                    return new Tuple2<Integer, String>(a._1, a._2.get());
                }
            };

    private static Tuple2<Integer,Tuple2<Integer,Integer>> combine(Tuple2<Integer,Tuple2<Integer,Integer>>){

    }

    public static JavaPairRDD<Integer, String> joinData(String t, String u){
        JavaRDD<String> transactionInputFile = sc.textFile(t);

        JavaPairRDD<Integer, Integer> transactionPairs = transactionInputFile.mapToPair(new PairFunction<String, Integer, Integer>() {
            public Tuple2<Integer, Integer> call(String s) {
                String[] transactionSplit = s.split("\t");
                return new Tuple2<Integer, Integer>(Integer.valueOf(transactionSplit[2]), Integer.valueOf(transactionSplit[1]));
            }
        });

        JavaPairRDD<Integer, Tuple2<Integer,Integer>> transactionPairs2 = transactionInputFile.mapToPair(new PairFunction<String, Integer, Tuple2<Integer,Integer>>() {
            public Tuple2<Integer, Tuple2<Integer, Integer>> call(String s) {
                String[] transactionSplit = s.split("\t");
                return new Tuple2<Integer, Tuple2<Integer, Integer>>(Integer.valueOf(transactionSplit[2]),new Tuple2<Integer,Integer>(Integer.valueOf(transactionSplit[1]), Integer.valueOf(transactionSplit[3])));
            }
        });

        JavaRDD<String> customerInputFile = sc.textFile(u);
        JavaPairRDD<Integer, String> customerPairs = customerInputFile.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String s) {
                String[] customerSplit = s.split("\t");
                return new Tuple2<Integer, String>(Integer.valueOf(customerSplit[0]), customerSplit[3]);
            }
        });

        JavaRDD<Tuple2<Integer,Optional<String>>> leftJoinOutput = transactionPairs.leftOuterJoin(customerPairs).values().distinct();
        //JavaRDD<Tuple2<Integer,Optional<String>>> leftJoinOutput2 = transactionPairs2.leftOuterJoin(customerPairs).values().distinct();
        transactionPairs2.aggregateByKey(0, new Function2<Tuple2<Integer,Tuple2<Integer,Integer>>, Tuple2<Integer,Tuple2<Integer,Integer>>, Tuple2<Integer,Tuple2<Integer,Integer>>>(){
            public Tuple2<Integer,Tuple2<Integer,Integer>> call( Tuple2<Integer,Tuple2<Integer,Integer>> value1 ,Tuple2<Integer,Tuple2<Integer,Integer>> value2){

            }
        });

        JavaPairRDD<Integer, String> output = leftJoinOutput.mapToPair(KEY_VALUE_PAIRER);

        return output;
    }
}
