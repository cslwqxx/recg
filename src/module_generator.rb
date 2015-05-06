unless (RbConfig::CONFIG['host_vendor'] =~ /android/i) === nil
  import 'dalvik.system.DexClassLoader'
end
#import android.os.Environment

class ModuleGenerator
	module EventDispatcher
		def setup_listeners
			@event_dispatcher_listeners = {}
		end

		def subscribe(event, &callback)
			(@event_dispatcher_listeners[event] ||= []) << callback
		end

		#protected
		def notify(event, *args)
			if @event_dispatcher_listeners[event]
				@event_dispatcher_listeners[event].each do |m|
					yield(m.call(*args)) if m.respond_to? :call
				end
			end
			return nil
		end
	end
	include EventDispatcher
  attr_reader :module_name, :module_id
  attr_accessor :job_id
  def initialize (module_name, module_id, code, rf_arr, rj_arr, res_arr, param_h, activity)
    @module_name = module_name
    @module_id = module_id
    @module_type = :normal
    @job_id = nil
    @route = nil
    @ins_pool = nil
    @sending_list = nil
    @receiving_list = nil
    @pause_flag = {}
    @exec_paused = false
    @migrate_dst_node_id = nil
    @params = param_h
    @current_session_id = nil
    @session_record = {}
    @processing_session_arr = []
    @accomplished_session_arr = []
    @curr_sending_block_time = 0
    if @module_name.to_s.start_with? 'Client'
      @module_type = :client
      @params[:throughput] = true
      @makespan_queue = Queue.new
      @makespan = 0
    end
    @params_version = -1
    @forwarding_params = nil
		@activity = activity
    @send_time = {}
    @unblock_send_quota = {}
    @recv_blocking_time = {}
    @sending_threads = {}
    @exec_thread = nil
    @exec_time = {:count=>0, :wait_spend => 0, :exec_spend => 0, :start_time => Time.now, :last_time => Time.now}
    @probe_thread = nil
    @dcl_arr = []
    if res_arr.length > 0
      res_arr.each do |r|
        instance_eval("#{r[:const_name]} = '#{r[:dir]}'\n")
      end
    end
    if rf_arr.length > 0
      rf_arr.each do |rf|
				instance_eval("require '#{rf}'\n")
				puts "File #{rf} required!"
      end
    end

    if rj_arr.length > 0
      rj_arr.each do |rj|
        if ServiceNode.instance.type == :client
          pcl ||= @activity.getClassLoader()
          dst ||= File.expand_path('../dex_dir')
          Dir.mkdir(dst) unless File.directory?(dst)
          @dcl_arr.push(DexClassLoader.new(rj, dst, nil, pcl))
          #p c = dcl.loadClass('ustc.lwq.FingerprintMaker')
          #p z=c.newInstance
          #p z.make(3)
          #p y=c.new
        else
          instance_eval("require '#{rj}'\n")
        end
        puts "Jar #{rj} required!"
      end
    end
    instance_eval(code)
  end

  def set_module_id (module_id)
=begin
    begin
      File.rename File.expand_path(@module_id, MODULES_ROOT_DIR), File.expand_path(module_id, MODULES_ROOT_DIR)
    rescue
      p $!
    end
=end
    @module_id = module_id
  end

  def prepare4migration (precursor_list, new_node_id)
    puts "Target instance on old node received the 'prepare for migration' notification!!"
    @pause_flag = precursor_list
    @migrate_dst_node_id = new_node_id
    #p @pause_flag
  end

  def route_renew (dst_module_name, ins_id, dst_id)
    puts "Precursor instance received 'route_renew' from Job_tracker!!"
    ins_id = ins_id.to_sym if ins_id.is_a? String
    @route[ins_id] = dst_id
    @sending_threads[ins_id].run
  end

  def pause_sending (dst_module_name, ins_id)
    puts "Precursor instance #{@module_name} received the 'pause_sending' from job_tracker, waiting sending_thread fall into sleep and send a pause_flag then."
    ins_id = ins_id.to_sym if ins_id.is_a? String
    old_node_id = @route[ins_id]
    @route[ins_id] = :need_migrating
    until @route[ins_id] == :migrating
      #p @route
      sleep 0.2
    end
    params = {
        :from => "#{@module_name}:#{@module_id}@#{ServiceNode.instance.uuid}",
        :to => "#{dst_module_name}:#{ins_id}@#{old_node_id}",
        :type => :msg,
        :content => {
            :pause_flag => true
        }
    }
    Communicator.instance.do_send params, false
    puts "Precursor instance #{@module_name} already sent pause flag!!"
  end

  def route_init (route)
    #p route
    @route = {}
    @ins_pool = {}
    @sending_list = {}
    route.each_pair do |module_name, dst_arr|
      @ins_pool[module_name.to_sym] = []
      dst_arr.each do |dst|
        module_id, dst_node_id = dst.split '@'
        module_id = module_id.to_sym
        @ins_pool[module_name.to_sym].push module_id
        @route[module_id] = dst_node_id
        if MAX_SENDING_LENGTH > 0
          @sending_list[module_id] = SizedQueue.new MAX_SENDING_LENGTH
        else
          @sending_list[module_id] = SizedQueue.new 1
        end

      end
    end
    if MAX_RECEIVING_LENGTH > 0
      @receiving_list = SizedQueue.new MAX_RECEIVING_LENGTH
    else
      @receiving_list = SizedQueue.new 1
    end

    #p @ins_pool
    #p @route
    #p @sending_list
    start_sending_threads
    start_exec_thread
  end

  def receive_func_call (msg_from, params, session_id)
    res = preprocess_inter_ins_comm msg_from, params, session_id
    return res if res == :paused
    blocking_start = Time.now
    from_sym = msg_from.to_sym
    @recv_blocking_time[from_sym] ||= {:count=>0, :blocking_spend => 0, :start_time => blocking_start, :last_time => blocking_start}
    process_data self, msg_from, params, blocking_start, session_id
  end

  def receive_msg (msg_from, params, session_id)
    res = preprocess_inter_ins_comm msg_from, params, session_id
    return res if res == :paused
    #p @exec_thread if @module_name.start_with? 'Image'
    from_sym = msg_from.to_sym
    blocking_start = Time.now
    @recv_blocking_time[from_sym] ||= {:count => 0, :blocking_spend => 0, :start_time => blocking_start, :last_time => blocking_start}
    unless MAX_RECEIVING_LENGTH > 0
      puts "#{@module_name} is waiting for receiving input."
      sleep 0.1 until @receiving_list.num_waiting > 0
      puts "#{@module_name} finished waiting for receiving input."
    end
    @receiving_list.push [msg_from, params, blocking_start, session_id]

    blocking_end = Time.now
    @recv_blocking_time[from_sym][:count] += 1
    @recv_blocking_time[from_sym][:blocking_spend] += blocking_end - blocking_start
    @recv_blocking_time[from_sym][:last_time] = blocking_end
    #puts "#{MAX_RECEIVING_LENGTH - @receiving_list.length} block-free msg remain..." if @module_name.start_with? 'Image'
    #p @exec_thread if @module_name.start_with? 'Image'
    #p @sending_threads  if @module_name.start_with? 'Image'
    #p @sending_list.values[0].length if @module_name.start_with? 'Image'
    #puts "pushed the msg into #{@module_name} recv queue![length=#{@receiving_list.length}]"
    MAX_RECEIVING_LENGTH - @receiving_list.length
  end

  def preprocess_inter_ins_comm (msg_from, params, session_id)
    #p params if @module_name.start_with? 'Image'
    if params.has_key?(:params_adjusting)
      #@forwarding_params = params[:params_adjusting]
      set_parameters params[:params_adjusting][:params], params[:params_adjusting][:version]
      finished_adjustment_report
    end
    if params[:pause_flag] and @pause_flag[msg_from]
      @pause_flag.delete(msg_from)
      puts "Target instance on old node received a 'pause_flag' from precursor instance, #{@pause_flag.length} more needed. "
      migration_start if @pause_flag.length == 0
      return :paused
    end
    unless msg_from.start_with? 'job_tracker'
      unless @module_type == :client
        @session_record[session_id] ||= {:send => {}, :recv => {}, :exec => []}
        unless @processing_session_arr[-1] == session_id
          @processing_session_arr.push session_id
        end
      end
    end
  end

  def migration_start
    Thread.new {
      puts 'Target instance on old node is waiting for exec_thread sleeping...'
=begin
      @exec_paused = true
      until @exec_thread.status === 'sleep'
        sleep 0.1
      end
=end
      #TODO:For test
      until @exec_thread.status === 'sleep' and @receiving_list.length == 0
        sleep 0.1
      end
      @sending_list.each_pair do |k,v|
        while v.length > 0 do
          sleep 0.1
        end
      end
      puts 'Target instance on old node start to migrate!!!'
      sending_list_arr = {}
      @sending_list.each_pair do |k,v|
        sending_list_arr[k] = []
        while v.length > 0 do
          sending_list_arr[k].push v.pop
        end
      end
      receiving_list_arr = []
      while @receiving_list.length > 0 do
        receiving_list_arr.push @receiving_list.pop
      end
      params = {}
      @params.each_key do |p_n|
        params[p_n] = instance_variable_get("@#{p_n}")
      end
      status_data = {
          :sending_list => sending_list_arr,
          :receiving_list => receiving_list_arr,
          :ins_pool => @ins_pool,
          :route => @route,
          :params_version => @params_version,
          :params => params,
          :current_session_id => @current_session_id,
          :session_record => @session_record,
          :processing_session_arr => @processing_session_arr,
          :accomplished_session_arr => @accomplished_session_arr,
          :user_data => self.main(:self, nil, :pack)
      }
      params = {
          :from => "#{@module_name}:#{@module_id}@#{ServiceNode.instance.uuid}",
          :to => "#{@module_name}:#{@module_id}@#{@migrate_dst_node_id}",
          :type => :migration,
          :content => {
              :data => status_data
          }
      }
      Communicator.instance.do_send params
      puts 'Target instance on old node successful send migration data to new node!!!'

    }
  end

  def receive_migration (msg_from, params)
    puts 'New node received migration data!!'
    #p msg_from
    data = params[:data]
    #p data
    @sending_list = {}
    data[:sending_list].each_pair do |k,v|
      if MAX_SENDING_LENGTH > 0
        @sending_list[k.to_sym] = SizedQueue.new MAX_SENDING_LENGTH
      else
        @sending_list[k.to_sym] = SizedQueue.new 1
      end

      v.each do |x|
        @sending_list[k.to_sym].push x
      end
    end
    #p @sending_list
    if MAX_RECEIVING_LENGTH > 0
      @receiving_list = SizedQueue.new MAX_RECEIVING_LENGTH
    else
      @receiving_list = SizedQueue.new 1
    end

    #p data['receiving_list'].length
    data[:receiving_list].each do |x|
      #p x
      @receiving_list.push x
    end
    #p @receiving_list
    @ins_pool = {}
    data[:ins_pool].each_pair do |k,v|
      @ins_pool[k.to_sym] = []
      v.each do |x|
        @ins_pool[k.to_sym].push x.to_sym
      end
    end
    #p @ins_pool
    @route = {}
    data[:route].each_pair do |k,v|
      @route[k.to_sym] = v
    end
    #p data
    set_parameters data[:params], data[:params_version]
    #@params_version = data[:param_version]
    #p @route
    @current_session_id = data[:current_session_id]
    @session_record = data[:session_record]
    @processing_session_arr = data[:processing_session_arr]
    @accomplished_session_arr = data[:accomplished_session_arr]
    self.main(:self, data[:user_data], :unpack)
    puts 'Target instance on new node finished unpacking migration data!!'
    start_sending_threads
    start_exec_thread
    puts 'Target instance on new node start running on new node!!'

    params = {
        :from => "#{@module_name}:#{@module_id}@#{ServiceNode.instance.uuid}",
        :to => "job_tracker:#{@job_id}",
        :type => :probe_info,
        :content => {
            :msg => :migration_finished
        }
    }
    Communicator.instance.do_send params
    puts "Target instance on new node successful send 'migration_finished' notification!"
  end

  def get_class_instance (class_name)
    instance = nil
    if ServiceNode.instance.type === :client
      @dcl_arr.each do |dcl|
        begin
          c = dcl.loadClass(class_name)
          instance = c.newInstance()
          break
        rescue
          p $!
        end
      end
    elsif ServiceNode.instance.type === :server
      tmp = class_name.gsub(/\.[a-z]/){|x| x[1].upcase }.sub(/\A[a-z]/){|x| x.upcase }.split('.')
      #p "Java::#{tmp[0]}::#{tmp[1]}.new"
      begin
        instance = eval("Java::#{tmp[0]}::#{tmp[1]}.new")
      rescue
        p $!
        $@.each {|e| puts e}
      end
      #p instance
    end
    instance
  end


  def func_call (dst_name, msg, field_key = nil)
    inter_ins_comm dst_name, msg, field_key, true
  end

  def send_to (dst_name, msg, field_key = nil)
    inter_ins_comm dst_name, msg, field_key, false
  end

  def inter_ins_comm (dst_name, msg, field_key = nil, block = false)
    params = {
        :from => "#{@module_name}:#{@module_id}@#{ServiceNode.instance.uuid}",
        :content => {
            :msg => msg
        }
    }
    if block
      params[:type] = :func_call
    else
      params[:type] = :msg
    end

    #p @module_type
    if @module_type == :client
      params[:session_id] = SecureRandom.uuid
      @session_record[params[:session_id]] = {:send => {}, :recv => {}, :exec => [], :makespan => 0}
      @processing_session_arr.push params[:session_id]
      puts "Generated new session id: #{params[:session_id]}"
    else
      params[:session_id] = @current_session_id
    end

    unless @forwarding_params == nil
      params[:content][:params_adjusting] = @forwarding_params
    end
    #p params[:content][:params_adjusting]
    dst_name = dst_name.to_sym if dst_name.is_a? String
    unless @ins_pool.has_key? dst_name
      #todo:发送目标模块非法
      return
    else

      module_ins_id = nil
      if @ins_pool[dst_name].length > 0
        if field_key === nil
          module_ins_id = @ins_pool[dst_name].sample
        elsif field_key.is_a? String
          hash = 0
          field_key.each_byte do |i|
            #equivalent to: hash = 65599*hash + (*str++);
            hash = i + (hash << 6) + (hash << 16) - hash
          end
          tmp = (hash & 0x7FFFFFFF) % @ins_pool[dst_name].length
          module_ins_id = @ins_pool[dst_name][tmp]
        else
        end
      else
        module_ins_id = @ins_pool[dst_name][0]
      end
      params[:to] = "#{dst_name}:#{module_ins_id}@"
      blocking_start = Time.now

      #if @module_name.start_with? 'Image'
        #p module_ins_id
        #p @sending_list
      #end
      res = nil
      if block
        res, = Communicator.instance.do_send params
      else
        unless MAX_SENDING_LENGTH > 0
          puts "#{@module_name} is waiting for sending output."
          sleep 0.1 until @sending_list[module_ins_id].num_waiting > 0
          puts "#{@module_name} finished waiting for sending output."
        end
        @sending_list[module_ins_id].push([deep_copy(params), blocking_start])
      end
      blocking_end = Time.now
      blocking_time = blocking_end - blocking_start
      #puts "blocking time = #{blocking_time}"
      unless @module_type == :client
        @exec_time[:exec_spend] -= blocking_time
        @curr_sending_block_time += blocking_time
      end
      #@sending_thread.run if @sending_thread.status === 'sleep'
      probe
      return res
    end

  end

  def sys_cmd (cmd)
    params = {
        :from => "#{@module_name}:#{@module_id}@#{ServiceNode.instance.uuid}",
        :to => "job_tracker:#{@job_id}",
        :type => :cmd,
        :content => {
            :msg => cmd
        }
    }
    Communicator.instance.do_send params
  end

  def clean
    params = {
        :from => "#{@module_name}:#{@module_id}@#{ServiceNode.instance.uuid}"
    }
    #p @route
    @probe_thread.kill unless @probe_thread == nil
    @ins_pool.each_pair do |dst_name,dst_arr|
      dst_arr.each do |dst|
        params[:to] = "#{dst_name}:#{dst}@#{@route[dst]}"
#        p params
        #TODO:此处将socket关闭的步骤暂时去掉了，因为有bug
        #Communicator.instance.close_socket params
      end
    end
    params[:to] = "job_tracker:#{@job_id}"
    #p params

    #TODO:此处将socket关闭的步骤暂时去掉了，因为有bug
    #Communicator.instance.close_socket params
    @ins_pool = nil
    @route = nil
    @job_id = nil
  end

  def set_parameters(param_h, version)
    if version > @params_version
      #puts "#{@module_name} is setting parameters!!"
      param_h.each do |k, v|
        if @params.has_key? k
          @params[k] = v
          instance_variable_set("@#{k}", v)
        end
      end
      @forwarding_params = {:params => param_h, :version => version} unless @params_version < 0
      @params_version = version
      #p @forwarding_params
      #finished_adjustment_report
    end
  end

  def start_parameters_adjustment (param_h, version)
    set_parameters param_h, version
    finished_adjustment_report
    #@forwarding_params ||= {:params => param_h, :version => version}
  end

  def finish_parameters_adjustment
    @forwarding_params = nil
  end

  private
  #TODO: 尽可能放进private

  def finished_adjustment_report
    params = {
        :from => "#{@module_name}:#{@module_id}@#{ServiceNode.instance.uuid}",
        :to => "job_tracker:#{@job_id}",
        :type => :probe_info,
        :content => {
            :msg => :adjustment_finished
        }
    }
    Communicator.instance.do_send params, false
  end

  def deep_copy(o)
    Marshal.load( Marshal.dump(o) )
  end

  def start_sending_threads
    @sending_list.each_key do |dst_ins_id|
      @sending_threads[dst_ins_id] ||= Thread.new (dst_ins_id) { |ins_id|
        begin
          loop do
            sending_data, waiting_start = @sending_list[ins_id].pop
            #p sending_data
            #dst_name, = sending_data[:to].split(':')
            if @route[ins_id] === :need_migrating
              @route[ins_id] = :migrating
              puts "Instance #{@module_name} paused sending data to #{sending_data[:to]}#{@route[ins_id]}"
              #p sending_data
              Thread.stop
              puts "Instance #{@module_name} restart sending data to #{sending_data[:to]}#{@route[ins_id]}"
            end
            sending_data[:to] += @route[ins_id].to_s
            dst = sending_data[:to].to_sym
            @send_time[dst] ||= {:count=> 0, :data_size => 0, :send_spend => 0, :sync_spend => 0, :blocking_spend => 0, :start_time => waiting_start, :last_time => waiting_start}
            send_start = Time.now
            @send_time[dst][:blocking_spend] += send_start - waiting_start
            @unblock_send_quota[dst] ||= 0
            #p @unblock_send_quota
            if @unblock_send_quota[dst] > 0
              tmp, data_size, sync_time, send_time = Communicator.instance.do_send sending_data, false#true
              @unblock_send_quota[dst] -= 1
            else
              tmp, data_size, sync_time, send_time = Communicator.instance.do_send(sending_data, true)
              @unblock_send_quota[dst] = tmp.to_i
              #puts "unblock_send_quota reset as #{@unblock_send_quota[dst]}"
            end
            send_end = Time.now

            session_id = sending_data[:session_id]
            #puts "#{@module_name}'s sending session_id :#{session_id}"
            @session_record[session_id] ||= {:send => {}, :recv => {}, :exec => [] }
            @session_record[session_id][:send][sending_data[:to]] ||= []
            @session_record[session_id][:send][sending_data[:to]].push [waiting_start, send_end, data_size, send_start - waiting_start, sync_time, send_time] #send_end - send_start - send_time]
            unless @module_type == :client
              while @processing_session_arr.length > 1 and session_id != @processing_session_arr[0]
                #puts "#{@processing_session_arr[0]} accomplished in #{@module_name} after sending."
                @accomplished_session_arr.push @processing_session_arr[0]
                @processing_session_arr.delete_at 0
              end
            end
            @send_time[dst][:count] += 1
            @send_time[dst][:data_size] += data_size unless data_size == :unknown
            @send_time[dst][:send_spend] += send_time
            @send_time[dst][:sync_spend] += sync_time #send_end - send_start - send_time
            @send_time[dst][:last_time] = send_end
          end
        rescue
          puts "Sending thread for #{@module_name}->#{dst_ins_id} encountered an error!"
          #puts "current session id: #{@current_session_id}"
          #p @processing_session_arr
          #p @accomplished_session_arr
          #p @session_record
          puts $!
          puts $@
        end
      }
    end

  end

  def start_exec_thread
    @exec_thread ||= Thread.new(self) { |m|
      begin
        #p @receiving_list.length
        puts "Instance #{@module_name} start exec with #{@receiving_list.length} waiting msg!!!"
        loop do
          Thread.stop if @exec_paused
          x = @receiving_list.pop
          #p x
          #p @receiving_list.length
          #if @module_name.to_s.start_with? 'Feature'
          #  puts "Receiving_list length: #{@receiving_list.length}"
          #end
          src, param, blocking_start, session_id = x
          #p session_id
          process_data m, src, param, blocking_start, session_id

        end
      rescue
        puts "Execution thread for #{@module_name} encountered an error!!"
        #puts "Current session id: #{@current_session_id}"
        #p @session_record
        puts $!
        puts $@
      end
    }
  end

  def probe
    @probe_thread ||= Thread.new {
      begin
        sleep PROBE_MONITOR_GAP
        loop do
          start_time = Time.now
          accomplished_session_records = {}

          accomplished_sessions = @accomplished_session_arr
          #puts "#{@module_name} : #{accomplished_sessions.length}"
          @accomplished_session_arr = []
          accomplished_sessions.each do |session_id|
            accomplished_session_records[session_id] = @session_record[session_id]
            @session_record.delete session_id
          end

=begin
            @accomplished_session_q.length.times do
              session_id = @accomplished_session_q.pop
              accomplished_session_records[session_id] = @session_record[session_id]
              @session_record.delete session_id
            end
=end
          params = {
              :from => "#{@module_name}:#{@module_id}@#{ServiceNode.instance.uuid}",
              :to => "job_tracker:#{@job_id}",
              :type => :probe_info,
              :content => {
                  :msg => {
                      :send_time => @send_time,
                      :exec_time => @exec_time,
                      :recv_blocking_time => @recv_blocking_time,
                      :accomplished_session_records => accomplished_session_records
                  }
              }
          }
          if @module_type == :client
            params[:content][:msg][:makespan] = @makespan
            @makespan = 0
          end

          #puts "#{@module_name} is sending probe information!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
          Communicator.instance.do_send params, false#, false
          #puts "#{@module_name} finished sending probe information@@@@@@@@@@@@@@@@@@@@@@@@@"
          end_time = Time.now
          @send_time.each_key do |des|
            @send_time[des] = {:count => 0, :data_size => 0, :send_spend => 0, :sync_spend => 0, :blocking_spend => 0, :start_time => end_time, :last_time => end_time}
          end
          @recv_blocking_time.each_key do |src|
            @recv_blocking_time[src] = {:count=>0, :blocking_spend => 0, :start_time => end_time, :last_time => end_time}
          end
          @exec_time = {:count=>0, :wait_spend => 0, :exec_spend => 0, :start_time => end_time, :last_time => end_time}
          gap = PROBE_MONITOR_GAP - (end_time - start_time)
          sleep gap if gap > 0
        end
      rescue
        puts "Probe thread for #{@module_name} encountered an error!!"
        puts $!
        puts $@
      end
    }
  end

  def process_data (m, src, param, blocking_start, session_id)
    @current_session_id = session_id
    unless session_id == nil
      params_copy = deep_copy @params
      #p params_copy
      #p @session_record
      @session_record[session_id] ||=  {:send => {}, :recv => {}, :exec => [] }
      @session_record[session_id][:params] = params_copy
      #p @session_record
    end

    @curr_sending_block_time = 0

    start_time = Time.now
    res = nil
    begin
      if param.has_key? :opt
        res = m.main src, param[:msg], param[:opt]
      else
        res = m.main src, param[:msg]
        if @module_type == :client
          curr_makespan = Time.now - @makespan_queue.pop
          @session_record[session_id][:makespan] = curr_makespan unless session_id == nil
          @makespan += curr_makespan
        end

      end
    rescue
      puts $!
      puts $@
    end

    end_time = Time.now
    #puts 'finished start msg!'
    unless session_id == nil
      @session_record[session_id] ||= {:send => {}, :recv => {}, :exec => [] }
      @session_record[session_id][:recv][src] ||= []
      @session_record[session_id][:recv][src].push [blocking_start, start_time]
      @session_record[session_id][:exec].push [start_time, end_time, @curr_sending_block_time]
    end
    if @module_type == :client
      #puts "#{session_id} accomplished in #{@module_name} after execution."

      @accomplished_session_arr.push session_id
      @processing_session_arr.delete session_id
    end
    @exec_time[:count] += 1
    @exec_time[:wait_spend] += start_time - blocking_start
    @exec_time[:exec_spend] += end_time - start_time
    @exec_time[:last_time] = end_time
    res
  end
end
