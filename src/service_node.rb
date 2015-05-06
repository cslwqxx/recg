require 'net/http'
require 'singleton'
require 'rbconfig'
require 'json'
require 'socket'
require 'fileutils'
require 'zip/zip'
require 'rexml/document'
include REXML
require_relative 'securerandom'
require_relative 'module_generator'
require_relative 'job_tracker'
require_relative 'env_observer'
require_relative 'communicator'

Net::HTTP.version_1_2
NODE_DELETE_THRESHOLD = 5 unless defined? NODE_DELETE_THRESHOLD
PING_SPREAD_NUM = 10 unless defined? PING_SPREAD_NUM
DELAY_CHECK_GAP = 0.3 unless defined? DELAY_CHECK_GAP
PING_TIME_OUT_THRESHOLD = 3 unless defined? PING_TIME_OUT_THRESHOLD
HEARTBEAT_GAP = 3 unless defined? HEARTBEAT_GAP
TCP_SOCKET_TIMEOUT_THRESHOLD = 5 unless defined? TCP_SOCKET_TIMEOUT_THRESHOLD
PROBE_MONITOR_GAP = 3 unless defined? PROBE_MONITOR_GAP
MODULE_RENEW_GAP = 3600 unless defined? MODULE_RENEW_GAP
MAX_SENDING_LENGTH = 3 unless defined? MAX_SENDING_LENGTH
MAX_RECEIVING_LENGTH = 3 unless defined? MAX_RECEIVING_LENGTH
FOG_MONITOR_IP = '210.45.64.154' unless defined? FOG_MONITOR_IP
FOG_MONITOR_PORT = 80 unless defined? FOG_MONITOR_PORT
PACKED_MODULES_ROOT_DIR = File.expand_path('../packed_modules') unless defined? PACKED_MODULES_ROOT_DIR
MODULES_ROOT_DIR = File.expand_path('../modules') unless defined? MODULES_ROOT_DIR
class ServiceNode
  include Singleton
  attr_reader :work_ip, :work_port, :uuid, :idle_module_hash, :type
	attr_accessor :activity
  def receive_cmd (dst_id, msg_from,  params)
    job_tracker = @job_tracker_hash[dst_id]
    job_tracker.receive_cmd msg_from, params[:msg]
    @job_tracker_hash.delete(dst_id) if params[:msg].to_sym === :exit
  end

  def alloc_job_tracker (params)
    p params
    #p @uuid
    client_id, app_name = params[:msg]
    #p client_id
    job_tracker = JobTracker.new client_id, app_name
    #p job_tracker
    @job_tracker_hash[job_tracker.job_id] = job_tracker
    #p @job_tracker_hash
    puts 'Have enough module:' + job_tracker.enough_module.to_s
  end

  def receive_probe_info (dst_id, msg_from,  params)
    job_tracker = @job_tracker_hash[dst_id]
    job_tracker.receive_probe_info msg_from, params[:msg]
  end

  def receive_migration (dst_id, msg_from,  params)
    ins = @running_module_hash[dst_id]
    ins.receive_migration msg_from, params
  end

  def receive_msg (dst_id, msg_from,  params, session_id)
    ins = @running_module_hash[dst_id]
    #p ins
    ins.receive_msg msg_from, params, session_id
#    p msg_from
  end

  def receive_func_call (dst_id, msg_from, params, session_id)
    ins = @running_module_hash[dst_id]
    ins.receive_func_call msg_from, params, session_id
  end

  def module_apply (params, job_id)
    module_name = params[:module_name]
    #p @idle_module_hash
    unless @idle_module_hash.has_key? module_name and @idle_module_hash[module_name].length > 0
      #todo:已被其他线程抢占，此处应该考虑线程安全问题
      puts "Requested idle module of #{module_name} is not found!"
      p @idle_module_hash.keys
      p @idle_module_hash[module_name].length
      return false
    end
    if params[:module_id] != nil and @running_module_hash.has_key? params[:module_id]
      puts 'ERROR: Module migration src and dst are same!'
      p params
      p @running_module_hash
      return false
    end
    ins = @idle_module_hash[module_name].pop
    if @idle_module_hash[module_name].length == 0
      @idle_module_hash.delete module_name
    end
    ins.job_id = job_id
    #@running_module_hash
    if params[:module_id] != nil and !@running_module_hash.has_key? params[:module_id]
      ins.set_module_id params[:module_id]
    end
    module_id = ins.module_id
    @running_module_hash[module_id] = ins
    module_id
  end

  def receive_instruct (module_id, params, job_id)
    case params[:opt]
      when :router_init
        router_init module_id, params, job_id
      when :init
        module_init module_id, params, job_id
      when :pause_sending
        pause_sending module_id, params, job_id
      when :route_renew
        route_renew module_id, params, job_id
      when :prepare4migration
        prepare4migration module_id, params, job_id
      when :instance_recycle
        begin
          #p "a"
          module_destroy module_id, params, job_id
          #p "b"
          #TODO:这里面有个bug，需要解决，暂时注释了
          #module_establish params[:module_name]
        rescue
          puts $!
          puts $@
        end

        p "c"
      when :destroy
        module_destroy module_id, params, job_id
      when :trigger_params_adjustment
        trigger_params_adjustment module_id, params, job_id
      when :finish_params_adjustment
        finish_params_adjustment module_id, params, job_id
      else
    end
  end



  def run_app (app_name)
    app_info = ManifestReader.read_app_manifest app_name
    full_module_name = (app_info[:client_module_name].to_s + '_'+ app_info[:app_name] + '_' + app_info[:author]).to_s
    module_download full_module_name unless  @local_packed_module_hash.has_key? full_module_name
    client_instance = module_establish full_module_name
    job_tracker_startup @uuid, app_name
		return client_instance
  end

  def job_finish (job_id)
    @job_tracker_hash.delete job_id
  end

  def wait
    @module_establish_thread.join
  end

  def initialize
    @idle_module_hash = {}
    @running_module_hash = {}
    @job_tracker_hash = {}
    @uuid = SecureRandom.uuid

    if (RbConfig::CONFIG['host_vendor'] =~ /android/i) === nil
      @type = :server
    else
      @type = :client
    end
=begin
    if @type === :server
      require '../android.jar'
      puts 'Required android.jar for server!'
    end
=end
		@activity = nil
    @work_ip = nil
    @work_port = nil
    @local_packed_module_hash = {}
    check_local_modules
    get_local_ip
    network_io_service
    EnvObserver.instance.start_heartbeat
    #res_hash = started_notification
    #EnvObserver.instance.addr_hash_init res_hash['addr_hash']
    #@downloadable_module_hash = res_hash['module_hash']
    #@downloadable_client_hash = res_hash['client_hash']
    #info_renew_heartbeat_service
    module_establish_service
  end

  def establish_client_modules (activity)
    @activity = activity
    num = 0
    @local_packed_module_hash.each_key do |name|
      puts "Detected a local packed module #{name}."
      module_establish name
      num += 1
    end
    EnvObserver.instance.module_loc_renew @uuid, @idle_module_hash.keys.map{|x| x.to_sym}
    num
  end

  private
	
  def get_local_ip
    Net::HTTP.start(FOG_MONITOR_IP, FOG_MONITOR_PORT) do |http|
      @work_ip = http.get('/get_local_ip').body
    end
  end

  def check_local_modules
		Dir.mkdir(PACKED_MODULES_ROOT_DIR) unless File.directory?(PACKED_MODULES_ROOT_DIR)
		Dir.foreach(PACKED_MODULES_ROOT_DIR) do |f|
			if f.end_with?('.zip')
        if @type == :client
          FileUtils.rm_r(File.expand_path(f, PACKED_MODULES_ROOT_DIR))#清空之前下载的模块（测试用）
        else
          module_name = f.split('.zip')[0]
          @local_packed_module_hash[module_name] = File.expand_path(f, PACKED_MODULES_ROOT_DIR)
        end
			end
		end
		Dir.mkdir(MODULES_ROOT_DIR) unless File.directory?(MODULES_ROOT_DIR)
		Dir.foreach MODULES_ROOT_DIR do |f|
			next if f === '.' or f === '..'
      if @type == :client
        FileUtils.rm_r(File.expand_path(f, MODULES_ROOT_DIR))#清空之前运行的残留
      end
    end
  end

  def network_io_service
    begin
      tcp_server = TCPServer.new(@work_ip,0)
      @work_port = tcp_server.addr[1]
      #udp_ds = UDPSocket.new
      #udp_ds.bind(@work_ip, @work_port)
    rescue
      tcp_server.close
      retry
    end

    #EnvObserver.instance.server_init udp_ds
    Communicator.instance.server_init tcp_server

  end

  def module_establish_service
    @module_establish_thread = Thread.new {
      loop do
        start_time = Time.now
        advised_module_arr = []
        Net::HTTP.start(FOG_MONITOR_IP, FOG_MONITOR_PORT) do |http|
          params = {}
          params[:uuid] = @uuid
          params[:addr] = @work_ip + ':' + @work_port.to_s
          #params[:delay_hash] = EnvObserver.instance.delay_hash
          params[:type] = self.type
          res = http.post('/get_module_establish_advice',params.to_json,{'Content-Type'=>'text/plain'}).body
          advised_module_arr = JSON.parse(res)
        end
        p advised_module_arr
        advised_module_arr.each do |am|
          unless @idle_module_hash.has_key? am
            module_download am
            module_establish am
          end
        end
        #sleep PROBE_MONITOR_GAP
        EnvObserver.instance.module_loc_renew @uuid, @idle_module_hash.keys.map{|x| x.to_sym}
        sleep MODULE_RENEW_GAP - (Time.now - start_time)
      end
    }
  end

  def job_tracker_startup (client_id, app_name)
    #candidate_arr = EnvObserver.instance.delay_hash.keys
    candidate_arr = EnvObserver.instance.addr_hash.keys
    if candidate_arr.length == 1 and candidate_arr[0] === "#{@work_ip}:#{@work_port}"
      puts "Failed to request remote job_tracker for #{app_name}, starting local job_tracker..."
      alloc_job_tracker({:msg => [client_id, app_name]})
    else
      res = Net::HTTP.start(FOG_MONITOR_IP, FOG_MONITOR_PORT) do |http|
        http.post('/run_app', [client_id, app_name, candidate_arr].to_json, {'Content-Type'=>'text/plain'})
      end
      case res
        when Net::HTTPSuccess
          puts "Successful requested a remote job_tracker for #{app_name}!"
        else
          puts "Failed to request remote job_tracker for #{app_name}, starting local job_tracker..."
          alloc_job_tracker({:msg => [client_id, app_name]})
        #job_tracker = JobTracker.new client_id, app_name
        #@job_tracker_hash[job_tracker.job_id] = job_tracker
        #puts 'Have enough module:' + job_tracker.enough_module.to_s
      end
    end
  end

  def module_download (am)
    unless @local_packed_module_hash.has_key? am
      Net::HTTP.start(FOG_MONITOR_IP, FOG_MONITOR_PORT) do |http|
        request = Net::HTTP::Get.new "/get_packed_module?name=#{am}"
        http.request request do |response|
          open File.expand_path("#{am}.zip",PACKED_MODULES_ROOT_DIR), 'wb' do |io|
            response.read_body do |chunk|
              io.write chunk
            end
          end
        end
      end
    end
  end

  def module_establish (am)
		module_instance = nil
    am = am.to_sym if am.is_a? String
    unless @idle_module_hash.has_key?(am) and @idle_module_hash[am].length > 0
      module_id = SecureRandom.uuid
      Dir.mkdir File.expand_path("#{module_id}", MODULES_ROOT_DIR)

      Zip::ZipFile.open(File.expand_path("#{am}.zip", PACKED_MODULES_ROOT_DIR)) do |zip_file|
        zip_file.each do |z|
          zip_file.extract(z, File.expand_path("#{module_id}/#{z.name}", MODULES_ROOT_DIR)) {true}
        end
      end

      #todo:将xml解析部分剥离到ManifestReader中
      info = {}
      module_xml_info = File.expand_path("#{module_id}/#{am}/ModuleManifest.xml", MODULES_ROOT_DIR)
      file = File.new(module_xml_info)
      doc = Document.new(file)
      root = doc.root
      info[:author] = root.attributes['author']
      info[:app_name] = root.attributes['app-name']
      info[:module_name] = root.attributes['module-name']
      info[:reusable] = root.attributes['reusable']
      rf_arr = []
      root.each_element 'require-file' do |rf|
        require_file_addr = File.expand_path "#{module_id}/#{am}/#{rf.attributes['name']}", MODULES_ROOT_DIR
        rf_arr.push require_file_addr
      end
      rj_arr = []
      tag = (self.type == :server) ? 'server-require-jar' : 'mobile-require-jar'
      root.each_element(tag) do |rf|
        require_file_addr = File.expand_path "#{module_id}/#{am}/#{rf.attributes['name']}", MODULES_ROOT_DIR
        rj_arr.push require_file_addr
      end
      res_arr = []
      root.each_element 'resource' do |r|
        res = {}
        res[:const_name] = r.attributes['const-name']
        res[:dir] = File.expand_path "#{module_id}/#{am}/#{r.attributes['dir']}", MODULES_ROOT_DIR
        res_arr.push res
      end
      param_h = {}
      root.each_element 'adjustable-parameter' do |ap|
        param_h[ap.attributes['name'].to_sym] = true
      end
      root.each_element 'entrance-file' do |ef|
        entrance_file_name = ef.attributes['name']
        code = File.read(File.expand_path("#{module_id}/#{am}/#{entrance_file_name}", MODULES_ROOT_DIR))
        module_instance = ModuleGenerator.new(am, module_id, code, rf_arr, rj_arr, res_arr, param_h, @activity)
        #sleep PROBE_MONITOR_GAP
        if @idle_module_hash.has_key? am
          @idle_module_hash[am].push module_instance
        else
          @idle_module_hash[am] = [module_instance]
        end

        break
      end
      file.close
      puts "Successful established module #{am}!"
    end
		module_instance
  end

  def module_destroy (module_id, params, job_id)
    ins = @running_module_hash[module_id]
    unless job_id === ins.job_id
      #todo:模块名与id号不匹配
      return false
    end
    ins.clean
    @running_module_hash.delete module_id
    retry_quota = 3
    begin
      #TODO:删除文件不干净，应该是要将动态加载的jar包去除掉之后再删
      #FileUtils.rm_r(File.expand_path(module_id, MODULES_ROOT_DIR))
    rescue
      p $!
      retry_quota -= 1
      retry if retry_quota > 0
    end
    retry_quota
  end

  def module_recycle (module_id, params, job_id)
    ins = @running_module_hash[module_id]
    #puts '1'
    unless job_id === ins.job_id
      #todo:模块名与id号不匹配
      return false
    end
    #puts '2'
    ins.clean
    #puts '3'
    @running_module_hash.delete module_id
    #puts '4'
    @idle_module_hash[ins.module_name].push ins
    #p ins
  end

  def router_init (module_id, params, job_id)
    #p params
    ins = @running_module_hash[module_id]
    #p @running_module_hash
    #p module_id
    unless job_id === ins.job_id
      #todo:模块名与id号不匹配
      return false
    end
    ins.route_init params[:route]
  end

  def prepare4migration (module_id, params, job_id)
    ins = @running_module_hash[module_id]
    unless job_id === ins.job_id
      #todo:模块名与id号不匹配
      return false
    end
    ins.prepare4migration params[:precursor_list], params[:new_node_id]
  end

  def route_renew (module_id, params, job_id)
    ins = @running_module_hash[module_id]
    puts "##########################"
    p job_id
    p ins.job_id
    unless job_id === ins.job_id
      #todo:模块名与id号不匹配
      return false
    end
    ins.route_renew params[:module_name], params[:ins_id], params[:dst_id]
  end

  def pause_sending (module_id, params, job_id)
    #p params
    ins = @running_module_hash[module_id]
    #p @running_module_hash
    #p module_id
    unless job_id === ins.job_id
      #todo:模块名与id号不匹配
      return false
    end
    ins.pause_sending params[:module_name], params[:ins_id]
  end

  def module_init (module_id, params, job_id)
    ins = @running_module_hash[module_id]
    unless job_id === ins.job_id
      #todo:模块名与id号不匹配
      return false
    end
    #p @running_module_hash
    ins.set_parameters params[:msg][:params], params[:msg][:version]
    ins.main 'job_tracker:'+job_id, nil, :init
  end

  def trigger_params_adjustment (module_id, params, job_id)
    ins = @running_module_hash[module_id]
    unless job_id === ins.job_id
      #todo:模块名与id号不匹配
      return false
    end
    #p @running_module_hash
    ins.start_parameters_adjustment params[:params], params[:version]
  end

  def finish_params_adjustment (module_id, params, job_id)
    ins = @running_module_hash[module_id]
    unless job_id === ins.job_id
      #todo:模块名与id号不匹配
      return false
    end
    #p @running_module_hash
    ins.finish_parameters_adjustment
  end

end

=begin
if __FILE__ == $0
  require_relative 'service_node'
  ServiceNode.instance
  sleep 5
#ServiceNode.instance.run_app 'Building'
  sleep
end
=end