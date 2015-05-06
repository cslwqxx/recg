require 'singleton'
require_relative 'service_node'

class EnvObserver
  include Singleton

  attr_reader :node_hash, :addr_hash

  def initialize
    @m_on_s_h = {}
    @heartbeat_thread = nil
    @addr_hash = {}
    @node_hash = nil
    @last_succ_send_timestamp = {}
  end

  def start_heartbeat
    @heartbeat_thread = Thread.new {
      loop do
        begin
          Net::HTTP.start(FOG_MONITOR_IP, FOG_MONITOR_PORT) do |http|
            res = http.post('/heartbeat',
                            ([ServiceNode.instance.uuid,
                              ServiceNode.instance.work_ip,
                              ServiceNode.instance.work_port,
                              ServiceNode.instance.type,
                              ServiceNode.instance.idle_module_hash.keys]).to_json,
                            {'Content-Type'=>'text/plain'})
            response_hash = JSON.parse(res.body)
            tmp_hash = {}
            #p @addr_hash
            @addr_hash.each do |k,v|
              if !response_hash['addr_hash'].has_key? k and
                  @last_succ_send_timestamp.has_key? k and
                  Time.now - @last_succ_send_timestamp[k] < HEARTBEAT_GAP
                tmp_hash[k] = v
              end
            end
            @addr_hash = response_hash['addr_hash']
            @addr_hash.merge!(tmp_hash) if tmp_hash.length > 0
            #p @addr_hash
            @node_hash = @addr_hash.invert
            response_hash['idle_module_list'].each do |uuid, idle_modules_arr|
              module_loc_renew(uuid,idle_modules_arr.map{|x| x.to_sym}) if @node_hash.has_key? uuid and idle_modules_arr != nil
            end
          end
        rescue
          puts $!
          puts $@
        end

        sleep HEARTBEAT_GAP
      end
    }
  end

  def succ_send(dst_addr)
    @last_succ_send_timestamp[dst_addr] = Time.now
  end

  def find_loc_by_module_name (module_name)
    available_module_arr = []
    if @m_on_s_h.has_key? module_name
      @m_on_s_h[module_name].each_pair do |uuid, v|
        if v
          available_module_arr.push uuid
        end
      end
    end
    available_module_arr
  end

  def module_loc_renew (uuid, idle_modules_arr)
    #p uuid
    #p idle_modules_arr
    #p @m_on_s_h
    idle_modules_arr.each do |m_name|
      @m_on_s_h[m_name] = {} if @m_on_s_h[m_name] === nil
      @m_on_s_h[m_name][uuid] = true
    end
    (@m_on_s_h.keys - idle_modules_arr).each do |m_name|
      @m_on_s_h[m_name][uuid] = false if @m_on_s_h[m_name].has_key?(uuid)
    end
  end

end
