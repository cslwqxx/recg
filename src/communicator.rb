require 'timeout'
require 'singleton'
require 'base64'
require_relative 'env_observer'
class Communicator
  include Singleton
  def initialize
    @tcp_server = nil
    @tcp_listener_thread = nil
    @tcp_client_hash = {}
#    @server_thread_hash = {}
  end
  def server_init (tcp_server)
    if @tcp_server === nil
      @tcp_server = tcp_server
    else
      #todo: warning duple init.
    end
    @tcp_listener_thread = Thread.new(tcp_server) do |server|
      puts "TCPServer startup!\n"
      loop do
        tcp_client = server.accept
        client_addr = tcp_client.addr[3] + ':' + tcp_client.addr[1].to_s
        Thread.start(tcp_client) do |client|
#          p @server_thread_hash
          p_from = nil
          p_to = nil
          dst_id = nil
          job_id = nil
          req_raw = ''
          #debug_flag = false
          begin
            puts "Accepted a new TCP client(#{client_addr})!!!"
            loop do
              #waiting_thread = nil
              req = nil
              res = ''
              begin
=begin
                start_time = Time.now
                begin # emulate blocking recvfrom
                  begin
                    req = ''
                    x = ''
                    begin
                      x = client.recv_nonblock(1)
                      req += x
                    end while x != "\n"
                    req = req.strip
                  end while req === ''

                rescue IO::WaitReadable
                  if Time.now - start_time < TCP_SOCKET_TIMEOUT_THRESHOLD
                    sleep 0.1
                    #IO.select([client],nil,nil,TCP_SOCKET_TIMEOUT_THRESHOLD)
                    retry
                  else
                    raise Timeout::Error
                  end
                end
=end
=begin
                Timeout::timeout(TCP_SOCKET_TIMEOUT_THRESHOLD) { #todo:常量化
                  waiting_thread = Thread.new {
                    Thread.current[:req] = client.gets
                  }
                  waiting_thread.join
                  req = waiting_thread[:req]
                }
=end
                #puts "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Waiting #{p_from}" if debug_flag
                req = client.gets
                #puts "||||||||||||||||||||||||||||||Got #{p_from}" if debug_flag
=begin
                while req_raw.index("\n") == nil
                  p req_raw.length
                  str = client.recv(65536)
                  req_raw += str
                end
                tmp = req_raw.index("\n")
                if tmp < req_raw.length
                  req = req_raw[0,tmp]
                  req_raw = req_raw[tmp+1..-1]
                else
                  req = req_raw
                  req_raw = ''
                end
                #p req
                #p req_raw
                #p req
=end
=begin
                len = client.gets.to_i
                #puts "receiving #{len} long msg!"
                req = ''
                req += client.gets while req.length < len
=end
                if req === 'heartbeat'
                  puts 'received heartbeat'
                  next
                end
                #params = JSON.parse req.strip
                #p req
                params = Marshal.load(Base64.decode64(req))
                #p params
              rescue JSON::ParserError
                client.puts 'bad msg!'
                client.flush
                puts "bad request (unable to parse): #{req}"
                next
              rescue Timeout::Error
                puts 'closing socket server due to waiting too long!'
                #p p_from
                #p p_to
                #p dst_id
                #p job_id
                #client.puts 'socket closing!'
                #client.flush
                waiting_thread.kill
                client.close
                p client
                #break
                self.exit
              end

              p_from ||= params[:from]
              p_to ||=params[:to]
              params[:from] ||= p_from
              params[:to] ||= p_to
              dst_id ||= params[:to].split(/[:@]/)[1]
              params[:dst_id] ||= dst_id

              if params[:type] == :probe_info
                debug_flag = true
              end

              cmd, msg = process_params params
              if cmd == :close
                client.puts msg
                client.flush
                client.close
                break
              elsif cmd == :res
                res = msg
              end

              #p params['block']
              if params[:block]
                #puts "reply:#{params.to_s}"
                #p '!!!!!!!!!!!' if params['type'] === 'close'
                #puts "reply:#{res}"
                client.puts res
                client.flush
              end
            end
          rescue
            p params
            puts "Rescue from tcp listener!!\tp_from:#{p_from}\tp_to:#{p_to}"
            puts $!
            p params
            puts $@
            client.puts 'socket closing!'
            client.flush
          ensure
            #@server_thread_hash.delete client.addr[3] + ':' + client.addr[1].to_s
            client.close
          end
        end
      end
    end
  end

  def do_send (params, block = true)
    pr = params.clone
    dst_id, dst_node_id = pr[:to].split('@')
    data_size = :unknown
    sync_time = 0
    send_time = 0
    #p dst_node_id
   # p EnvObserver.instance.node_hash
    dst_addr = nil
    tmp_start = Time.now
    begin
      dst_addr = EnvObserver.instance.node_hash[dst_node_id]
      sleep 0.1 if dst_addr == nil
    end until dst_addr != nil or Time.now - tmp_start > HEARTBEAT_GAP * 5
    if dst_addr == nil
      p dst_node_id
      p EnvObserver.instance.node_hash
      return [:error, data_size, sync_time, send_time]
    end
    h_key = "#{dst_id}*#{dst_addr}&#{pr[:from]}"
    pr[:block] = block
    src_id, src_node_id = pr[:from].split('@')
    if src_node_id === dst_node_id
      pr[:dst_id] = pr[:to].split(/[:@]/)[1]
      cmd, msg = process_params pr
      if block and cmd == :res
        return [msg, data_size, sync_time, send_time]
      end
    else
      p_to = nil
      p_from = nil
      retry_quota = 3
      begin
        if @tcp_client_hash.has_key? h_key
          p_to ||= pr[:to]
          p_from ||= pr[:from]
          pr.delete :to
          pr.delete :from
        else
          pr[:to] ||= p_to
          pr[:from] ||= p_from
          host, port = dst_addr.split ':'
          @tcp_client_hash[h_key] = TCPSocket.open host, port

          #p @tcp_client_hash
        end
        s = @tcp_client_hash[h_key]

        #str = pr.to_json
        #puts 'sending msg!'
        #p pr
        sync_start = Time.now
        str = Base64.encode64(Marshal.dump(pr)).gsub("\n", '')# + "\n"
        sync_time = Time.now - sync_start
        begin
          #s.sendmsg_nonblock "#{str.length}\n#{str}\n"
          #puts str
          data_size = str.length
          start_time = Time.now
          #s.send str, 0
          #s.flush
          s.puts str
          s.flush
          send_time = Time.now - start_time
          #p data_size
          #s.puts str.length
          #s.puts str
          #s.flush
        rescue
          p '@@@@@@@'
          puts $!
          #p params
          #puts $@
          s.close
          @tcp_client_hash.delete h_key
          raise
        end

        EnvObserver.instance.succ_send(dst_addr)

        if block
          res = s.gets
          res = res.chop unless res === nil
          #puts "Received reply #{res}"
          if res === 'bad msg!'
            raise
          elsif res === 'socket closing!'
            s.close
            @tcp_client_hash.delete h_key
          else
            #p res
            return [res, data_size, sync_time, send_time]
          end
        end
      rescue
        #puts $!
        #p pr
        retry_quota -= 1
        if retry_quota > 0
          puts "#{p_from} is retrying to send data to #{p_to}."
          retry
        end
      end
    end
    return [:no_res, data_size, sync_time, send_time]
  end

  def close_socket (params)
    pr = params.clone
    dst_id, dst_node_id = pr[:to].split('@')
    dst_addr = EnvObserver.instance.node_hash[dst_node_id]  #todo:用于单点容灾
    h_key = "#{dst_id}*#{dst_addr}&#{pr[:from]}"
    #puts "h_key=#{h_key}"
    unless @tcp_client_hash.has_key? h_key
      return
    end
    pr.delete :to
    pr.delete :from

    s = @tcp_client_hash[h_key]
    #p @tcp_client_hash
    tmp_p = pr
    tmp_p[:type] = :close
    tmp_p[:block] = true
    #p tmp_p
    begin
      res = nil
      count = 0
      begin
        #puts 'a'
        str = Base64.encode64(Marshal.dump(tmp_p)).gsub("\n", '') + "\n"
        #s.puts str.length
        #s.puts str
        #puts "closing #{str}"
        s.send str, 0
        #p @server_thread_hash
        #s.recv_nonblock 1000
        #s.puts tmp_p.to_json
        #puts 'b'
        #s.flush
        #puts 'c'
        Timeout::timeout(5) {
          res = s.gets
        }
        res.strip! unless res === nil
        count += 1
      end while res === 'bad msg!'
      unless res === 'socket closing!'
        puts "res:#{res}"
        puts "count:#{count.to_s}"
        puts "receive:#{tmp_p.to_s}"
        puts 'remote socket fail to close!'
      end
    rescue
      #p params
      #puts $!
      #puts $@
    ensure
      s.close
      @tcp_client_hash.delete h_key
      #puts 'current tcp_client_hash:'
      #p @tcp_client_hash
    end
  end

  private
  def process_params (params)
    p_from = params[:from]
    dst_id = params[:dst_id]
    content = params[:content]
    case params[:type]
      when :close
        #puts 'Got a close request!'
        #p  params
=begin
                  begin
                    sleep 0.5
                    tmp = client.recv_nonblock 65536
                    puts "recv the tail of socket: #{tmp.chop}"
                  rescue Errno::EWOULDBLOCK
                    #do nothing
                  end
=end
        return [:close, 'socket closing!']
      when :pause_flag
        ServiceNode.instance.receive_pause_flag dst_id, p_from, content
      when :cmd
        ServiceNode.instance.receive_cmd dst_id, p_from, content
      #break if content['msg'].to_sym === :exit
      when :alloc_job_tracker
        ServiceNode.instance.alloc_job_tracker content
        return [:close, 'Successful allocating remote job_tracker!']
      when :probe_info
        ServiceNode.instance.receive_probe_info dst_id, p_from, content
      when :migration
        ServiceNode.instance.receive_migration dst_id, p_from, content
      when :msg
        #puts 'Module received a msg!'
        if params.has_key? :session_id
          session_id = params[:session_id]
        else
          session_id = nil
        end
        res = ServiceNode.instance.receive_msg dst_id, p_from, content, session_id
        return [:res, res]
      when :func_call
        if params.has_key? :session_id
          session_id = params[:session_id]
        else
          session_id = nil
        end
        res = ServiceNode.instance.receive_func_call dst_id, p_from, content, session_id
        return [:res, res]
      when :instruct
        job_id = p_from.split(':')[1]
        if content[:opt] == :module_apply
          module_id = ServiceNode.instance.module_apply content, job_id
          res = module_id
          return [:res, res]
        else
          #p content
          ServiceNode.instance.receive_instruct dst_id, content, job_id
        end
      else
    end
    return [:non_res, true]
  end
end