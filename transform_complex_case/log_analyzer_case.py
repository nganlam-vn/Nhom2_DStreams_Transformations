from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import logging

ENABLE_DEMO_1 = 1   # lọc message trùng
ENABLE_DEMO_2 = 0   # log message blacklist

if __name__ == "__main__":

    conf = SparkConf().setAppName("LogAnalyzer_Transform_Demo").setMaster("local[2]")
    conf.set("spark.ui.showConsoleProgress", "false")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    logging.getLogger("py4j").setLevel(logging.ERROR)
    
    ssc = StreamingContext(sc, 5) 

    logs_dstream = ssc.socketTextStream("localhost", 9999)
    errors_dstream = logs_dstream.filter(lambda line: "ERROR" in line)

    batch_counter = {"count": 0}
    def next_batch(rdd):
        batch_counter["count"] += 1
        return batch_counter["count"]


    # CASE 1: Lọc message trùng và chỉ giữ lại 1 log 

    if ENABLE_DEMO_1:
        distinct_dstream = errors_dstream.transform(
            lambda rdd: rdd.map(lambda log_line: (
                                    " - ".join(log_line.split(" - ")[2:]),  
                                    log_line                               
                                ))
                        .reduceByKey(lambda first, _duplicate: first)  
                        .values() 
        )

        distinct_dstream.foreachRDD(lambda rdd:
            print(f"\n=== BATCH {next_batch(rdd)}: DISTINCT ERRORS ===\n" + "\n".join(rdd.collect()))
        )


    # CASE 2: Đánh dấu và hiển thị log có chứa IP/user trong blacklist

    if ENABLE_DEMO_2:

        blacklist = ["192.168.1.100", "203.0.113.42", "user_123456"]

        blacklist_dstream = logs_dstream.transform(
            lambda rdd: rdd.filter(lambda line: any(b in line for b in blacklist))
                        .map(lambda line: "[BLACKLISTED] " + line)
        )

        blacklist_dstream.foreachRDD(lambda rdd:
            print(f"\n=== BATCH {next_batch(rdd)}: BLACKLISTED LOGS ===\n" + "\n".join(rdd.collect()))
        )

    # Start streaming
    ssc.start()
    ssc.awaitTermination() 
