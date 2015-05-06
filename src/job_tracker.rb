require 'json'
require 'securerandom'
require 'net/http'
require 'rexml/document'
include REXML
require_relative 'env_observer'
require_relative 'communicator'
require_relative 'manifest_reader'
require_relative 'optimizer'
APP_MANIFESTS_ROOT_DIR = File.expand_path('../app_manifests')

class JobTracker
  attr_reader :enough_module, :job_id
  def initialize (client_id, app_name)
    @using_modules_hash = {}
    @job_id = SecureRandom.uuid
    @client_id = client_id
    set_performance_measurer app_name
    @app_info = ManifestReader.read_app_manifest app_name
    puts "Launching job tracker of #{app_name}..."
    available_modules_hash = {}
    @establish_type_hash = {}
    @core_num = {}
    @core_num.default = 2
    @migration_status = :no_task
    @migration_task = {}
    @params_version = 0
    @params_adjustment_status = :no_task
    @params = {}
    #TODO: delete this tmp_direction
    @tmp_direction = 1
    @params_adjustment_task = {}
    @bandwidth_record = {}
    @bandwidth_record.default = 0
    @bandwidth = 0
    @workload_record = {}
    @overload_node = {}
    @overload_ins = {}
    @last_probe_time = {}
    @last_probe_time.default = Time.now
    @ins_prob_from = {}
    @detailed_prob_info_arr = []
    @congestion_check = {}
    @throughput_adjust_scale = 0
    @current_throughput = 0
    @current_makespan = 0
    @current_performance = 0
    @network_congestion_prob = 0
    @network_congestion_timestamp = Time.now
    @last_migration_time = Time.now
    @establish_type_hash.default = :single
    @optimizer = Optimizer.new app_name
    @app_info[:app_module_hash].each_value do |m|
      unless m[:name] === @app_info[:client_module_name]
        global_module_name = (m[:name].to_s + '_' + app_name + '_' + @app_info[:author]).to_sym
        available_modules_hash[m[:name]] = EnvObserver.instance.find_loc_by_module_name global_module_name
      end
      m[:related_module].each_pair do |name, param_arr|
        if param_arr[1] === 'Grouping'
          @establish_type_hash[name] = :multi
        end
      end
    end
    p available_modules_hash
    available_modules_hash.each_key do |name|
      if @establish_type_hash[name] === :single
        available_modules_hash[name] = available_modules_hash[name].sample 1
      end
      if available_modules_hash[name].length == 0
        puts "available_modules_hash:"
        p available_modules_hash
        @enough_module = false
        return
      end
    end
    #p available_modules_hash
    @enough_module = true
    job_params = {}
    @app_info[:param].each do |k,v|
      job_params[k] = v[:default_value]
    end
    @params = job_params
    @params[:throughput] = 0.35#0.35
    @params[:compress_ratio] = 0.1#0.35
    @reports = Queue.new
    #TODO: record the network condition exp: {"component":"Network_Condition", "bandwidth":25, "rtt":0, "timestamp":1415600000.848}
    @report_thread = Thread.new {
      open('exec_report.data', 'w') {}
      begin
#        Net::HTTP.start('119.81.140.203', 8080) do |http|
            loop do
              report = @reports.pop
              if ServiceNode.instance.type == :server
                open('exec_report.data', 'a') { |f|
                  f.puts report
                  f.flush
                }
              end
              #res = http.post('/1.0/monitoring/data?tenant=bluemix_customer&origin=polyu',report,{'Content-Type'=>'text/plain'}).body
              #puts res
            end
#        end
      rescue
        puts "Report thread of #{@app_info[:app_name]} encountered an error!"
        puts $!
        puts $@
      end
    }
    module_init_notification available_modules_hash, '_'+app_name+'_'+@app_info[:author], @params
    app_start
  end

  def receive_cmd (cmd_from, cmd)
    #todo:是否需要将指令发送权下放到左右module？
    #todo:在此记录app的状态，并根据状态对不同的cmd进行权限设置
    #p cmd_from
    #p cmd
    #todo:当cmd为exit的时候，需要按照拓扑结构进行关闭
    if @client_id === cmd_from.split(':')[1]
      Thread.new {
        params = {
            :from => 'job_tracker:'+ @job_id + '@' + ServiceNode.instance.uuid,
            :type => :msg,
            :content => {
                :opt => cmd
            }
        }
        @using_modules_hash.each_pair do |module_name, module_arr|
          module_arr.each do |m|
            params[:to] = module_name.to_s + ':' + m
            Communicator.instance.do_send params
          end
        end
        if cmd === :exit
          #p '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
          params[:type] = :instruct
          @using_modules_hash.each_pair do |module_name, module_arr|
            #p module_name
            module_arr.each do |m|
              params[:to] = module_name.to_s + ':' + m
              if @app_info[:app_module_hash][module_name][:reusable] === 'true'
                params[:content][:opt] = :recycle
              else
                params[:content][:opt] = :destroy
              end
              #p params
              Communicator.instance.do_send params
            end
          end
          #p '------------------------------------------'
          params.delete :content
          params.delete :type
          @using_modules_hash.each_pair do |module_name, module_arr|
            module_arr.each do |m|
              params[:to] = module_name.to_s + ':' + m
              #TODO:此处将socket关闭的步骤暂时去掉了，因为有bug
              #Communicator.instance.close_socket params
            end
          end
          ServiceNode.instance.job_finish @job_id
          #p '=========================================='
        end
      }
    end
  end
	
  def receive_probe_info(probe_info_from, probe_info)
    #puts "------------------------#{probe_info_from}!"
    case probe_info
      when :adjustment_finished
        if @params_adjustment_status == :adjusting
          if @params_adjustment_task.has_key? probe_info_from
            #p @params_adjustment_task
            @params_adjustment_task.delete probe_info_from
            if @params_adjustment_task.length == 0
              finish_params_adjustment
              puts "The #{@params_version}th time parameter adjustment is finished."
              p @params
              @params_adjustment_status = :no_task
            end
          else
            #puts "Received a 'adjustment_finished' info but NO related probe_info_from!"
            #p probe_info_from
            #p @params_adjustment_task
          end
        else
          #puts "Received a 'adjustment_finished' info but NO related adjustment task!"
          return
        end
      when :migration_finished
        if  @migration_task.has_key?(probe_info_from) and
              @migration_task[probe_info_from][0] != :finished
          puts "Job_tracker received 'migration_finished' from target instance on old node, starting renew precursors' route!"
          #p probe_info_from
          #p @migration_task
          module_full_name, ins_id, src_id, dst_id, precursor_list = @migration_task[probe_info_from]
          params = {
              :from => "job_tracker:#{@job_id}@#{ServiceNode.instance.uuid}",
              :type => :instruct,
              :content => {
                  :opt => :route_renew,
                  :module_name => module_full_name,
                  :ins_id => ins_id,
                  :dst_id => dst_id
              }
          }
          #p precursor_list
          precursor_list.each_key do |pre|
            params[:to] = pre
            #p "!!!!!!!!!!!!!!!!!!"
            #p pre
            #p params
            Communicator.instance.do_send params
          end
          @migration_task[probe_info_from].insert(0, :finished)

          puts "Job_tracker successful sent 'route_renew' instructs to all precursors!"

          #clean_old_ins
          params = {
              :from => "job_tracker:#{@job_id}@#{ServiceNode.instance.uuid}",
              :to => "#{module_full_name}:#{ins_id}@#{src_id}",
              :type => :instruct,
              :content => {
                  :opt => :instance_recycle,
                  :module_name => module_full_name,
                  :ins_id => ins_id,
              }
          }
          Communicator.instance.do_send params
          puts "Job_tracker successful sent 'instance_recycle' instruct to old node!"

          #p @workload_record
          #p @overload_ins

          src_id = src_id.to_sym
          ins_id = ins_id.to_sym
          if @workload_record[src_id].has_key? ins_id
            @workload_record[src_id][:total] -= @workload_record[src_id][ins_id]
            @workload_record[src_id].delete ins_id
          end
          @overload_ins.delete ins_id if @overload_ins.has_key? ins_id
          name, = module_full_name.to_s.split('_')
          #puts '-'*30
          #p name
          #p "#{src_id}@#{ins_id}"
          @using_modules_hash[name.to_sym].delete "#{ins_id}@#{src_id}"

          @migration_status = :no_task
          return
        else
          puts "Received a 'migration_finished' info but NO related migration task!"
          return
        end
      else
        normal_probe_processing probe_info_from, probe_info
      end
  end

  private

  def app_start
    params = {
        :from => 'job_tracker:'+ @job_id + '@' + ServiceNode.instance.uuid,
        :to => @app_info[:client_module_name].to_s + ':' + @client_id,
        :type => :msg,
        :content => {
            :opt => :start
        }
    }
    Communicator.instance.do_send params
  end

  def module_init_notification (available_modules_h, module_name_suffix, job_params)

    module_apply available_modules_h, module_name_suffix

    router_init

    module_init job_params

  end

  def module_replace (module_full_name, ins_id, src_id, dst_id)
    # module_apply
    module_name, = module_full_name.to_s.split('_')
    module_name = module_name.to_sym
    params = {
        :from => 'job_tracker:'+ @job_id + '@' + ServiceNode.instance.uuid,
        :to => 'root@' + dst_id,
        :type => :instruct,
        :content => {
            :opt => :module_apply,
            :module_name => module_full_name,
            :module_id => ins_id
        }
    }
    puts "src_id:#{src_id}\ndst_id:#{dst_id}"
    p params
    res, = Communicator.instance.do_send params
    p res
    unless res === ins_id
      puts 'ERROR: Module migration FAILed!'
      return false
    end
    puts "Job_tracker successful booked an #{module_name} instance on new node!"

    # set to waiting for migrate (old ins)
    precursor_list = {}
    @app_info[:app_module_hash][module_name][:precursor_module].each do |module_name|
      #p module_name
      @using_modules_hash[module_name].each do |m|
        precursor = "#{module_name}_#{@app_info[:app_name]}_#{@app_info[:author]}:#{m}"
        precursor_list[precursor] = true
      end
    end
    params = {
        :from => "job_tracker:#{@job_id}@#{ServiceNode.instance.uuid}",
        :to => "#{module_full_name}:#{ins_id}@#{src_id}",
        :type => :instruct,
        :content => {
            :opt => :prepare4migration,
            :precursor_list => precursor_list.clone,
            :new_node_id => dst_id
        }
    }
    Communicator.instance.do_send params

    puts "Job_tracker successful send 'prepare for migration' notification to target instance on old node!"
    #p @using_modules_hash
    @using_modules_hash[module_name].push ins_id + '@' + dst_id
    #set_pause_flag
    params = {
        :from => "job_tracker:#{@job_id}@#{ServiceNode.instance.uuid}",
        :type => :instruct,
        :content => {
            :opt => :pause_sending,
            :module_name => module_full_name,
            :ins_id => ins_id
        }
    }
    #p precursor_list
    precursor_list.each_key do |pre|
      params[:to] = pre
      Communicator.instance.do_send params
    end
    #p precursor_list
    puts "Job_tracker successful send 'pause sending' notification to all precursor instances for target instance!"

    @migration_task["#{module_full_name}:#{ins_id}@#{dst_id}"] = [
        module_full_name, ins_id, src_id, dst_id, precursor_list
    ]
    puts 'Job_tracker is waiting for migration finished!'
    #status_data_transfer
    #router_init
    #set_resume_flag
    #Communicator.instance.close_socket params
    true
  end

  def module_apply  (available_modules_h, module_name_suffix)

   # p available_modules_h
    available_modules_h.each_pair do |module_name, available_loc_arr|
      module_full_name = (module_name.to_s + module_name_suffix).to_sym
      params = {
          :from => 'job_tracker:'+ @job_id + '@' + ServiceNode.instance.uuid,
          :type => :instruct,
          :content => {
              :opt => :module_apply,
              :module_name => module_full_name
          }
      }
      @using_modules_hash[module_name] = []
      available_loc_arr.each do |e|
        params[:to] = 'root@' + e
        res, = Communicator.instance.do_send params
        #p res
        #p e
        @using_modules_hash[module_name].push res + '@' + e
      end
    end
   # p available_modules_h
    params = {
        :from => "job_tracker:#{@job_id}@#{ServiceNode.instance.uuid}",
        :to => "root@#{@client_id}",
        :type => :instruct,
        :content => {
            :opt => :module_apply,
            :module_name => (@app_info[:client_module_name].to_s + module_name_suffix).to_sym
        }
    }
    res, = Communicator.instance.do_send params
    @using_modules_hash[@app_info[:client_module_name]] = [res + '@' + @client_id]
    @client_id = res + '@' + @client_id
=begin
    params = {
        :from => "job_tracker:#{@job_id}@#{ServiceNode.instance.uuid}",
        :to => "root@#{@client_id}",
    }
    Communicator.instance.close_socket params
=end
  end

  def router_init
    params = {
        :from => 'job_tracker:'+ @job_id + '@' + ServiceNode.instance.uuid,
        :type => :instruct,
        :content => {
            :opt => :router_init
        }
    }
    @using_modules_hash.each_pair do |module_name, module_arr|
      #params[:content][:module_name] = (module_name.to_s + module_name_suffix).to_sym
      params[:content][:route] = {}
      @app_info[:app_module_hash][module_name][:related_module].each_key do |name|
        params[:content][:route][name] = @using_modules_hash[name]
      end
      #p module_arr
      module_arr.each do |m|
        params[:to] = module_name.to_s + ':' + m
        Communicator.instance.do_send params
      end
    end
  end

  def module_init (job_params)
    #p @using_modules_hash
    @using_modules_hash.each_pair do |module_name, available_loc_arr|
      params = {
          :from => 'job_tracker:'+ @job_id + '@' + ServiceNode.instance.uuid,
          :type => :instruct,
          :content => {
              :opt => :init,
              :msg => {:params => job_params, :version => @params_version}
          }
      }
      available_loc_arr.each do |e|
        params[:to] = "#{module_name}:#{e}"
        Communicator.instance.do_send params
      end
    end
  end

  def trigger_params_adjustment(parameters)
    if @params_adjustment_status == :no_task and @migration_status == :no_task
      @params_adjustment_status = :adjusting
      @params_version += 1
      #p parameters
      @params_adjustment_task = {}
      @using_modules_hash.each do |module_name, ins_arr|
        ins_arr.each do |m|
          ins_name = "#{module_name}_#{@app_info[:app_name]}_#{@app_info[:author]}:#{m}"
          @params_adjustment_task[ins_name] = true
        end
      end
      #p @params_adjustment_task
      params = {
          :from => "job_tracker:#{@job_id}@#{ServiceNode.instance.uuid}",
          :to => "Client:#{@client_id}",
          :type => :instruct,
          :content => {
              :opt => :trigger_params_adjustment,
              :params => parameters,
              :version => @params_version
          }
      }
      Communicator.instance.do_send params
    else
      return :adjustment_conflicted
    end

  end

  def finish_params_adjustment
    params = {
        :from => "job_tracker:#{@job_id}@#{ServiceNode.instance.uuid}",
        :type => :instruct,
        :content => {
            :opt => :finish_params_adjustment,
        }
    }
    @using_modules_hash.each do |module_name, ins_arr|
      ins_arr.each do |m|
        params[:to] = "#{module_name}_#{@app_info[:app_name]}_#{@app_info[:author]}:#{m}"
        Communicator.instance.do_send params
      end
    end
  end

  def trigger_module_migration(module_full_name_str, addr)
    if @params_adjustment_status == :no_task and @migration_status == :no_task
      @last_migration_time = Time.now
      # and probe_info['exec_time']['time_spend']/probe_info['exec_time']['count'] > 0.05

      ins_id, src_id = addr.split '@'
      module_name, = module_full_name_str.split '_'
      module_name = module_name.to_sym
      module_full_name = module_full_name_str.to_sym
      if @establish_type_hash[module_name] == :single
        p module_full_name
        replaceable_module_arr = EnvObserver.instance.find_loc_by_module_name module_full_name
        #p replaceable_module_arr
        #p EnvObserver.instance.node_hash
        if replaceable_module_arr.length > 0
          puts '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
          @migration_status = :running
          dst_id = replaceable_module_arr.sample
          ans = module_replace module_full_name, ins_id, src_id, dst_id
          if ans
            return true
          else
            @migration_status = :no_task
          end
        end
      end
      #available_modules_hash[m[:name]] = EnvObserver.instance.find_loc_by_module_name global_module_name
    end
    false
  end

  def set_performance_measurer(app_name)
    Dir.mkdir(APP_MANIFESTS_ROOT_DIR) unless File.directory?(APP_MANIFESTS_ROOT_DIR)
    measurer_dir = File.expand_path("#{app_name}_performance_measurer.rb", APP_MANIFESTS_ROOT_DIR)
    Net::HTTP.start(FOG_MONITOR_IP, FOG_MONITOR_PORT) do |http|
      request = Net::HTTP::Get.new "/get_app_performance_measurer?name=#{app_name}"
      http.request request do |response|
        open measurer_dir, 'w' do |io|
          response.read_body do |chunk|
            io.write chunk
          end
        end
      end
    end
    instance_eval(File.read(measurer_dir))
  end

  def normal_probe_processing (probe_info_from, probe_info)
    @detailed_prob_info_arr << [probe_info_from, probe_info]
    receive_time = Time.now
    full_name, ins_id, node = probe_info_from.split(/[:@]/)
    node = node.to_sym
    ins = ins_id.to_sym
    @ins_prob_from[ins] = [full_name, ins, node]
    name, = full_name.split('_')

    #puts "Received a normal probe from #{name}!"

    unless @workload_record.has_key?(node)
      @workload_record[node] = {}
      @workload_record[node][:total] = 0
      @workload_record[node].default = 0
    end
    probe_gap = [receive_time - @last_probe_time[probe_info_from], PROBE_MONITOR_GAP, probe_info[:exec_time][:last_time] - probe_info[:exec_time][:start_time]].max
    workload = probe_info[:exec_time][:exec_spend] / probe_gap
    ratio = 0.5 + [0.2 * probe_info[:exec_time][:count], 0.4].min
    @workload_record[node][:total] -= @workload_record[node][ins]
    @workload_record[node][ins] = @workload_record[node][ins] * (1-ratio) + workload * ratio
    @workload_record[node][:total] += @workload_record[node][ins]

    @overload_node[node] = receive_time if @workload_record[node][:total] > 0.9 * @core_num[node]
    @overload_ins[ins] = receive_time if @workload_record[node][ins] > 0.9

    if full_name.start_with?('Client')
      throughput = probe_info[:exec_time][:count] / probe_gap
      @current_throughput = @current_throughput * (1-ratio) + throughput * ratio
      if probe_info[:exec_time][:count] > 0
        makespan = probe_info[:makespan] / probe_info[:exec_time][:count]
        @current_makespan = @current_makespan * (1-ratio) + makespan * ratio
      end
      #puts "Current throughput: #{@current_throughput}\tCurrent makespan: #{@current_makespan}"
    end
    #puts "Get a probe info from #{full_name}:"
    #p probe_info[:accomplished_session_records]
    if node.to_s == @client_id.split('@')[1]
      loc = 'mobile'
    else
      loc = 'cloud'
    end
    if full_name.start_with?('Client')
      send_session_records probe_info[:accomplished_session_records], full_name, ins, loc, throughput
    else
      send_session_records probe_info[:accomplished_session_records], full_name, ins, loc
    end


    #if probe_info[:exec_time][:count] > 0 and full_name.start_with?('Feature')
    #  puts "@@:#{name}:\nBlock Time:#{probe_info[:recv_blocking_time].values[0][:blocking_spend]}\tWait Time:#{probe_info[:exec_time][:wait_spend]}\tCount:#{probe_info[:exec_time][:count]}\tLoad:#{@workload_record[node][ins]}"
    #end
    workload_str = ''
    @workload_record.each do |k,v|
      if k.to_s == @client_id.split('@')[1]
        node_type = 'Client'
      else
        node_type = 'Server'
      end
      workload_str += "\n\t#{node_type} : #{v[:total]}"
    end

    probe_info[:send_time].each do |k,v|
      dst_name, dst_ins, dst_node = k.to_s.split(/[:@]/)
      link_name = "#{ins_id}->#{dst_ins}".to_sym
      bandwidth = v[:data_size]/[receive_time - @last_probe_time[probe_info_from], PROBE_MONITOR_GAP, v[:last_time] - v[:start_time]].max
      bandwidth = (bandwidth/1000).to_i
      @bandwidth -= @bandwidth_record[link_name].to_i
      ratio = 0.5 + [0.2 * v[:count], 0.4].min
      @bandwidth_record[link_name] = @bandwidth_record[link_name] * (1-ratio) + bandwidth * ratio
      @bandwidth += @bandwidth_record[link_name].to_i
      if bandwidth > 0 and v[:blocking_spend]/v[:count] > 0.1
        @congestion_check[dst_ins] = [ins_id, receive_time]
      end
      #puts "@@:#{name}:"
      #puts "#{link_name} :\tBandwidth:#{bandwidth}\tCount:#{v[:count]}\tDataSize:#{v[:data_size]}\tSendSpend:#{v[:send_spend]}\tSyncSpend:#{v[:sync_spend]}\tBlockingSpend:#{v[:blocking_spend]}"
    end
    #p @congestion_check
    if @congestion_check.has_key?(ins_id)
      src_id, last_time = @congestion_check[ins_id]
      if receive_time - last_time > 1.5 * PROBE_MONITOR_GAP
        @congestion_check.delete ins_id
      else
        probe_info[:recv_blocking_time].each do |k,v|
          from_name, from_ins, from_node = k.to_s.split(/[:@]/)
          if from_ins === src_id
            if v[:count] > 0 and v[:blocking_spend] == 0
              @network_congestion_prob = 0.5 * (1 + @network_congestion_prob)
            else
              @network_congestion_prob *= 0.5
            end
            @network_congestion_timestamp = receive_time
          end
        end
      end
    end

    #puts "Current Bandwidth:#{@bandwidth}\nCurrent Workload:#{workload_str}"

    status_check



    #Parameters adjustment
=begin
        if probe_info_from.start_with? ('FeatureExtractor') and @params_adjustment_status != :adjusting
          avg_time = probe_info[:exec_time][:time_spend]/probe_info[:exec_time][:count]
          target_time = 1
          if avg_time < 0.8*target_time
            @params_adjustment_status = :adjusting

            @params.each do |k,v|
              max = @app_info[:param][k][:max]
              min = @app_info[:param][k][:min]
              step = [max - v, v - min].min
              #type = @app_info[:param][k][:type]
              @params[k] = v + step * (target_time-avg_time) / target_time
            end
            @params_version += 1
            trigger_params_adjustment @params, @params_version
          elsif avg_time > 1.2*target_time
            @params_adjustment_status = :adjusting
            @params.each do |k,v|
              max = @app_info[:param][k][:max]
              min = @app_info[:param][k][:min]
              step = [max - v, v - min].min
              #type = @app_info[:param][k][:type]
              @params[k] = v - step * [(avg_time-target_time)/target_time, 1].min
            end
            @params_version += 1
            trigger_params_adjustment @params, @params_version
          end
        end
=end
    @last_probe_time[probe_info_from] = receive_time
  end

  def status_check
    @status_check_thread ||= Thread.new {
      begin
        loop do
          check_timestamp = Time.now

          @overload_node.each { |k,v| @overload_node.delete k if check_timestamp - v  > 2 * PROBE_MONITOR_GAP }
          @overload_ins.each { |k,v| @overload_ins.delete k if check_timestamp - v > 2 * PROBE_MONITOR_GAP }
          if check_timestamp - @network_congestion_timestamp > 1.5 * PROBE_MONITOR_GAP
            @network_congestion_prob *= 0.5
            @network_congestion_timestamp = Time.now
          end

          params = @params.clone
          params[:throughput] = @current_throughput
          params[:makespan] = @current_makespan
          @current_performance = measure_performance params
          puts "Current performance: #{@current_performance}"

=begin
        p params = @params.clone
        params.delete :throughput if params.has_key? :throughput
        p probe_info = {
            :app_name => @app_info[:app_name],
            :workload_record => @workload_record,
            :using_modules_hash => @using_modules_hash,
            :detailed_probe_info => @detailed_prob_info_arr,
            :params => params,
            :throughput => @current_throughput,
            :makespan => @current_makespan
        }

        Net::HTTP.start(FOG_MONITOR_IP, FOG_MONITOR_PORT) do |http|
          res = http.post('/probe_info',
                          (probe_info).to_json,
                          {'Content-Type'=>'text/plain'})
          @detailed_prob_info_arr = []
          p response_hash = JSON.parse(res.body)
        end
=end
          #p @overload_ins
          #p @workload_record
=begin
        if @network_congestion_prob < 0.5
          client_node_id = @client_id.split('@')[1].to_sym
          flag = false
          @workload_record[client_node_id].each_key do |m|
            unless m == :total
              if @overload_ins.has_key? m
                module_full_name, ins, node = @ins_prob_from[m]
                trigger_module_migration module_full_name, "#{ins}@#{node}"
                flag = true
                break
              end
            end
          end
          unless flag
            @workload_record[client_node_id].each_key do |m|
              unless m == :total
                module_full_name, ins, node = @ins_prob_from[m]
                tmp = trigger_module_migration module_full_name, "#{ins}@#{node}"
                break if tmp
              end
            end
          end
        end
=end
=begin
          if @overload_ins.length > 0
            @overload_ins.each_key do |ins|
              module_full_name, ins, node = @ins_prob_from[ins]
              if node.to_s == @client_id.split('@')[1]
                trigger_module_migration module_full_name, "#{ins}@#{node}"
              end
            end
          end
=end
          #TODO: delete this temp processing and resume the upper method
=begin
          if @params_version == 7
            @params[:throughput] = 2
            @params[:compress_ratio] = 0.28
            @ins_prob_from.each_value do |v|
              module_full_name, ins, node = v
              if (module_full_name.start_with?('Preprocess') or module_full_name.start_with?('Image'))and
                  node.to_s == @client_id.split('@')[1]
                trigger_module_migration module_full_name, "#{ins}@#{node}"
              end
            end

          end
=end

#puts 'Start to adjust throughput!'
          throughput_adjustment

          sleep PROBE_MONITOR_GAP
        end
      rescue
        puts "Status check thread of #{@app_info[:app_name]} encountered an error!"
        @status_check_thread = nil
        puts $!
        puts $@
      end

    }
  end

  def throughput_adjustment
    #p @migration_status
    if @params_adjustment_status == :no_task and @migration_status == :no_task
      puts "network congestion prob: #{@network_congestion_prob.round(2)}"
      tmp_str = ''
      @overload_ins.each_key do |ins|
        module_full_name,= @ins_prob_from[ins]
        module_name, = module_full_name.split '_'
        tmp_str += module_name
      end
      puts "overload ins: #{tmp_str}"
      puts "overload node: #{@overload_node.length}"
      #p @workload_record
      v = @params[:compress_ratio]
=begin
      max = @app_info[:param][:compress_ratio][:max]
      min = @app_info[:param][:compress_ratio][:min]
      v = @params[:compress_ratio]
      if @overload_ins.length > 0 or @overload_node.length > 0 or @network_congestion_prob > 0.5
        @throughput_adjust_scale = 0
        @params[:throughput] = @current_throughput * 0.9
        if @current_throughput < 0.5
          step = v - min
          @params[:compress_ratio] = v - step * (1 - @current_throughput)
        end

      elsif @current_throughput > @params[:throughput] * 0.8
        @throughput_adjust_scale += 0.4
        @params[:throughput] = @current_throughput + 2**@throughput_adjust_scale - 1

        if @params[:throughput] > 0.8
          step = max - v
          @params[:compress_ratio] = v + step * [@params[:throughput] - 0.5, 1].min
        end

      else
        #TODO:recover this 'return'
        return
      end
=end
=begin
      if @overload_ins.length > 0 or @overload_node.length > 0 or @network_congestion_prob > 0.5
        if @current_throughput < 0.5 * @params[:throughput]
          @params[:throughput] = 0.4
          @params[:compress_ratio] = 0.2
        end
        @params[:throughput] *= @current_throughput < 0.9 * @params[:throughput] ? 0.95 : 0.98

      else
        @params[:throughput] *= 1.02
      end

      @params[:throughput] = 0.2 if @params[:throughput] < 0.2
=end

      #TODO:delete those settings
      change = @tmp_direction * rand(9)/200.0
      @params[:compress_ratio] = v + change
      if @params[:compress_ratio] < 0.1
        @tmp_direction = 1
        @params[:compress_ratio] = v - (v - 0.1)/2
      elsif @params[:compress_ratio] > 0.2
        @tmp_direction = -1
        @params[:compress_ratio] = v + (0.2 - v)/2
      end
      @params[:compress_ratio] = 0.15

      trigger_params_adjustment @params
    end
  end

  def send_session_records (records, module_name, instance_id, module_loc, throughput=nil)
    app_suffix = "_#{@app_info[:app_name]}_#{@app_info[:author]}"
    records.each do |session_id, record|
      next if record == nil or session_id == nil
      report = {
          :component => module_name,
          :instance_id => instance_id,
          :params => record[:params],
          :location => module_loc,
          :events => []
      }
      if module_name.start_with?('Client')
        report[:makespan] = record[:makespan]
        report[:throughput] = throughput
      end
      record[:recv].each do |recv_from, times|
        name, = recv_from.split(':')
        src_name = name# + app_suffix
        times.each do |time_pair|
          report[:events] << {
              :type => :received,
              :timestamp => time_pair[0].to_f,
              :token => session_id,
              :source => src_name
          }
        end
      end
      record[:exec].each do |time_pair|
        report[:events] << {
            :type => :started,
            :timestamp => time_pair[0].to_f,
            :token => session_id
        }
        report[:events] << {
            :type => :finished,
            :timestamp => time_pair[1].to_f,
            :blocked_time => time_pair[2],
            :token => session_id
        }
      end
      record[:send].each do |send_to, infos|
        name, = send_to.split(':')
        dst_name = name + app_suffix
        infos.each do |info|
          report[:events] << {
              :type => :sent,
              :timestamp => info[1].to_f,
              :data_size => info[2],
              :blocked_time => info[3].to_f,
              :sync_time => info[4].to_f,
              :send_time => info[5].to_f,
              :token => session_id,
              :destination => dst_name
          }
        end
      end
      @reports << report.to_json
    end
  end
end
