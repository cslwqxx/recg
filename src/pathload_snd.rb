require 'socket'

HOST = 'localhost'
PORT = 3000

i = 0
until $*[i] == nil
  if $*[i] == '-d'
    DEBUG = true
  elsif $*[i] == '-q'
    QUIET = true
  end
  i += 1
end
QUIET = false unless defined? QUIET
DEBUG = false unless defined? DEBUG

# Time.now latency
latency = Array(30)
30.times do |i|
  t1 = Time.now.to_f
  t2 = Time.now.to_f
  latency[i] = t2 - t1
end
latency.sort
time_now_latency = latency[15]
if DEBUG
  puts "DEBUG :: time_now_latency = #{time_now_latency}"
end


# Control stream: TCP connection
sock_tcp = TCPServer.new(HOST, PORT)
sock_udp = UDPSocket.new
sock_udp.bind(HOST, PORT)
iterate = true

begin
  puts 'Waiting for receiver to establish control stream => ' unless QUIET

  # Wait until receiver attempts to connect, starting new measurement cycle
  tcp_client = sock_tcp.accept
  puts 'OK' unless QUIET
  local_time = Time.now
  puts "Receiver #{tcp_client.addr[2]} starts measurements at #{local_time}"  unless QUIET

  # Connect UDP socket
  sock_udp.connect *tcp_client.addr.values_at(3,1)




end while iterate