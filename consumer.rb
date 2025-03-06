require 'kafka'

kafka_url = ENV['KAFKA_URL'] || 'localhost:9092'
kafka = Kafka.new([kafka_url], client_id: "consumer_app")

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

