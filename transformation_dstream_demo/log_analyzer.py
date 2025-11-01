# =============================================================================
# SPARK DSTREAMS: LỌC VÀ CHUẨN HÓA LOG SERVER
# Mục đích: Nhận log từ socket → filter() ERROR → map() chuẩn hóa → lưu file
# Demo: Cách sử dụng filter() và map() trong DStreams thực tế
# =============================================================================

from pyspark import SparkContext, SparkConf           # Spark Core
from pyspark.streaming import StreamingContext       # Spark Streaming
import re                                            # Regex cho xử lý text
import logging                                       # Tắt log spam   

if __name__ == "__main__":
    # Khởi tạo Spark
    conf = SparkConf().setAppName("LogAnalyzer_DStreams").setMaster("local[2]")
    conf.set("spark.ui.showConsoleProgress", "false")  # Tắt progress bar
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")  # Chỉ hiển thị lỗi quan trọng
    
    # Tắt log spam
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
    logging.getLogger("org.spark_project").setLevel(logging.ERROR)
    
    # Tạo StreamingContext: xử lý mỗi 5 giây
    ssc = StreamingContext(sc, 5)
    batch_count = 0  # Đếm batch

    # Kết nối socket để nhận log
    logs_dstream = ssc.socketTextStream("localhost", 9999)
    print("Kết nối thành công với socket localhost:9999")
    print("Raw ERROR logs sẽ được lưu vào file: raw_errors.txt")

    # BƯỚC 1: filter() - Lọc ra chỉ dòng có "ERROR"
    errors_dstream = logs_dstream.filter(lambda line: "ERROR" in line)

    # Lưu ERROR gốc vào file (chưa chuẩn hóa)
    def save_raw_errors(time, rdd):
        collected = rdd.collect()
        if collected:
            with open("raw_errors.txt", "a", encoding="utf-8") as f:
                for error_line in collected:
                    f.write(error_line + "\n")

    errors_dstream.foreachRDD(save_raw_errors)  # Lưu file

    # BƯỚC 2: map() - Chuẩn hóa ERROR thành chữ hoa
    normalized_dstream = errors_dstream.map(lambda line: line.upper())

    # Hiển thị kết quả theo batch
    def print_with_batch_number(time, rdd):
        global batch_count
        batch_count += 1
        collected = rdd.collect()
        if collected:
            print(f"\n=== BATCH {batch_count} ===")
            for error in collected:
                print(error)
        else:
            print(f"\n=== BATCH {batch_count} === (Không có ERROR)")

    normalized_dstream.foreachRDD(print_with_batch_number)  # In kết quả
    
    # Bắt đầu streaming
    ssc.start()
    ssc.awaitTermination()
