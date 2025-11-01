# =============================================================================
# Lọc ERROR trong HEAD tránh trường hợp có ERROR nhiễu trong message (Info 198.51.100.107 GET /metrics - average error rate 0.9 percent)
# Trích IP, PATH, MESSAGE từ log 
# =============================================================================
import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import logging
import ipaddress

if __name__ == "__main__":
    # Spark
    conf = SparkConf().setAppName("LogAnalyzer_DStreams").setMaster("local[2]")
    conf.set("spark.ui.showConsoleProgress", "false")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    # Tắt log spam
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
    logging.getLogger("org.spark_project").setLevel(logging.ERROR)

    ssc = StreamingContext(sc, 2)
    batch_count = 0

    logs = ssc.socketTextStream("localhost", 9999)
    print("Kết nối thành công với socket localhost:9999")
    
    # Tìm IP 
    def find_ip(head):
        for t in head.split():
            try:
                ipaddress.IPv4Address(t)
                return t
            except ipaddress.AddressValueError:
                continue
        return "NO IP"
    
    # FILTER: chỉ nhận dòng match HEAD/MESSAGE và HEAD có token 'error'
    errors = logs.filter(
        lambda line:(
            len(parts := line.rsplit("-", 1)) == 2 and "error" in parts[0].lower()         
        )
    )
    # MAP: trích (IP, PATH, MESSAGE) HEAD/MESSAGE
    parsed = errors.map(
        lambda line: 
        (
            (lambda head, msg: 
            (
                find_ip(head), 
                next((t for t in head.split() if t.startswith("/")), "NO PATH"), 
                msg.strip()
            )
            ) (*(line.rsplit("-", 1)))
        )
    )
    # In kết quả mỗi batch
    def print_with_batch_number(time, rdd):
        global batch_count
        batch_count += 1
        rows = rdd.take(100)
        
        if rows:
            print(f"\nBATCH {batch_count} : {str(time)}")
            for ip, path, msg in rows:
                print(f"IP     : {ip}")
                print(f"Path   : {path}")
                print(f"Message: {msg}")
                print("-"*50)
        else:
            print(f"\nBATCH {batch_count} : {str(time)} : (Không có ERROR)")
            print("-"*50)
    parsed.foreachRDD(print_with_batch_number)

    ssc.start()
    ssc.awaitTermination()
