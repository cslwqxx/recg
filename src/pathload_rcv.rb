require 'socket'
require 'basic_socket'

HOST = 'localhost'
PORT = 3000

# set up control channel
socket_tcp = TCPSocket.open HOST, PORT

reuseaddr = socket_tcp.getsockopt(:SOCKET, :REUSEADDR).bool
optval = socket_tcp.getsockopt(Socket::SOL_SOCKET,Socket::SO_REUSEADDR)
optval = optval.unpack "i"
reuseaddr = optval[0] == 0 ? false : true

p socket_tcp.setsocketopt Socket::IPPROTO_TCP, Socket::TCP_MAXSEG



