require 'kafka'

# Get Kafka URL from Heroku environment variables
kafka_urls = ENV['KAFKA_URL'].split(',')

# Remove 'kafka+ssl://' prefix to make it compatible with ruby-kafka
kafka_brokers = kafka_urls.map { |url| url.sub(/^kafka\+ssl:\/\//, '') }

# Configure Kafka client with SSL options
kafka = Kafka.new(
  kafka_brokers,
  client_id: "consumer_app",
  ssl_ca_cert: ENV['KAFKA_TRUSTED_CERT'],
  ssl_client_cert: ENV['KAFKA_CLIENT_CERT'],
  ssl_client_cert_key: ENV['KAFKA_CLIENT_CERT_KEY']
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

