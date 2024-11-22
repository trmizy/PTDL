FROM confluentinc/cp-kafka-connect:6.2.0

# Sao chép MongoDB Kafka Connector và các file jar cần thiết vào thư mục plugins
COPY kafka-plugins/mongo-kafka-connect-1.13.0-confluent.jar /usr/share/java/

# Đặt CONNECT_PLUGIN_PATH để chỉ tới thư mục chứa các plugins
ENV CONNECT_PLUGIN_PATH="/usr/share/java"

# Đảm bảo Kafka Connect khởi động bình thường
CMD ["/etc/confluent/docker/run"]
