require 'kafka'

# Get Kafka URLs from Heroku environment
kafka_urls = ENV['KAFKA_URL'].split(',')

# Remove 'kafka+ssl://' prefix for compatibility with ruby-kafka
kafka_brokers = kafka_urls.map { |url| url.sub(/^kafka\+ssl:\/\//, '') }

# Kafka Client Configuration with SSL
kafka = Kafka.new(
  kafka_brokers,
  client_id: "consumer_app",
  ssl_ca_cert: ENV['KAFKA_TRUSTED_CERT'],        # SSL CA Certificate
  ssl_client_cert: ENV['KAFKA_CLIENT_CERT'],     # SSL Client Certificate
  ssl_client_cert_key: ENV['KAFKA_CLIENT_CERT_KEY'], # SSL Client Key
  ssl_verify_hostname: false  # <-- Disable hostname verification
)

consumer = kafka.consumer(group_id: "books_group")
consumer.subscribe("books")

puts "Consumer started, waiting for messages..."

trap("TERM") { consumer.stop }
trap("INT") { consumer.stop }

begin
  consumer.each_message do |message|
    puts "Received message: #{message.value}"
  end
rescue Kafka::ProcessingError => e
  puts "Error processing message: #{e}"
end
