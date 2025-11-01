from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import logging

conf = SparkConf().setAppName("LogAnalyzer_TransformDemo").setMaster("local[*]")
conf.set("spark.ui.showConsoleProgress", "false")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
logging.getLogger("py4j").setLevel(logging.ERROR)

ssc = StreamingContext(sc, 5)  

logs_dstream = ssc.socketTextStream("localhost", 9999)
errors_dstream = logs_dstream.filter(lambda line: "ERROR" in line)

batch_counter = {"count": 0}
def next_batch():
    batch_counter["count"] += 1
    return batch_counter["count"]

blacklist = ["192.168.1.100", "203.0.113.42", "user_123456"]

def transform_batch(rdd):
    if rdd.isEmpty():
        return rdd

    def extract_message(log_line):
        return " - ".join(log_line.split(" - ")[2:])

    # Step 1: Lọc trùng theo message và giữ chỉ giữ lại một log
    distinct_rdd = (
        rdd.map(lambda log: (extract_message(log), log))   
           .reduceByKey(lambda first, _: first)            
           .values()                                       
    )

    # Step 2: Đánh dấu và hiển thị log có chứa IP/user trong blacklist
    def mark_blacklist(log_line):
        if any(b in log_line for b in blacklist):   
            return log_line + " [BLACKLISTED]"
        return log_line

    marked_rdd = distinct_rdd.map(mark_blacklist)

    return marked_rdd


transformed_dstream = errors_dstream.transform(transform_batch)

def print_results(rdd):
    batch_id = next_batch()
    final_logs = rdd.collect()
    
    if final_logs:
        print(f"\n=== BATCH {batch_id}: DISTINCT -> BLACKLIST LOGS ===")
        for log in final_logs:
            print(log)
    else:
        print(f"\n=== BATCH {batch_id}: NO ERRORS LOGS ===")

transformed_dstream.foreachRDD(print_results)

ssc.start()
ssc.awaitTermination()
